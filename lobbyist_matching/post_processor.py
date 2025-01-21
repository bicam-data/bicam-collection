import logging
import asyncio
import asyncpg
from typing import List
from tqdm import tqdm
from collections import defaultdict

from matcher import (
    ReferenceMatcher,
    load_corpus_bills,
    calculate_title_similarity
)
from paragraph_processor import process_paragraphs

"""
This module handles post-processing of bill reference matches to improve accuracy and handle edge cases.
It includes functions for:
- Processing unmatched references by checking bill number variations
- Deduplicating matches within sections
- Correcting wrong title matches by checking nearby congresses
- Improving low confidence matches
- Processing paragraph-level matches
"""

async def process_unmatched_refs(pool: asyncpg.Pool, run_id: int) -> None:
    """
    Process unmatched references for potential bill number variations.
    
    Checks unmatched references against bill number variants that differ by a single digit,
    looking for high confidence title matches. This helps catch OCR errors in bill numbers.
    
    Args:
        pool: Database connection pool
        run_id: ID of the current matching run
    """
    try:
        logging.info("Starting unmatched reference processing...")
        
        async with pool.acquire() as conn:
            # Get unmatched references with bill numbers
            unmatched_refs = await conn.fetch("""
                SELECT 
                    r.reference_id,
                    r.title,
                    r.bill_type,
                    regexp_replace(r.bill_id, '[^0-9]', '', 'g') AS bill_number,
                    r.congress_number,
                    r.filing_uuid,
                    r.section_id
                FROM lobbied_bill_matching.extracted_references r
                LEFT JOIN lobbied_bill_matching.reference_matches m 
                    ON r.reference_id = m.reference_id
                WHERE r.run_id = $1
                AND (m.match_type = 'unmatched' OR m.match_id IS NULL)
                AND r.bill_id IS NOT NULL
                AND r.title IS NOT NULL
            """, run_id)

            if not unmatched_refs:
                logging.info("No unmatched references to process")
                return

            # Load corpus bills
            bills_data = await conn.fetch("""
                WITH bill_titles AS (
                    SELECT 
                        b.congress::TEXT,
                        b.bill_type,
                        b.bill_number::TEXT,
                        array_agg(DISTINCT t.title) FILTER (WHERE t.title IS NOT NULL) as titles
                    FROM bicam.bills b
                    LEFT JOIN bicam.bills_titles t ON b.bill_id = t.bill_id
                    GROUP BY b.congress, b.bill_type, b.bill_number
                    
                    UNION
                    
                    SELECT 
                        b.congress_num::TEXT as congress,
                        b.bill_type,
                        b.bill_number::TEXT,
                        array_agg(DISTINCT t.bill_title) FILTER (WHERE t.bill_title IS NOT NULL) as titles
                    FROM relational___congress.bills b
                    LEFT JOIN relational___congress.bill_titles t ON b.bill_id = t.bill_id
                    GROUP BY b.congress_num, b.bill_type, b.bill_number
                )
                SELECT * FROM bill_titles
            """)
            
            # Build bill lookup
            bills_by_congress = defaultdict(lambda: defaultdict(dict))
            for bill in bills_data:
                bills_by_congress[bill['congress']][bill['bill_type']][bill['bill_number']] = bill

            # Process each unmatched reference
            matches_to_insert = []
            for ref in tqdm(unmatched_refs, desc="Processing unmatched references"):
                if not ref['congress_number'] or not ref['bill_number']:
                    continue

                # Get all single-digit variants
                variants = get_single_digit_variants(ref['bill_number'])
                best_match = None
                best_sim = 0

                # Check each variant
                for test_num in variants:
                    congress_bills = bills_by_congress.get(ref['congress_number'], {})
                    bill_type_bills = congress_bills.get(ref['bill_type'], {})
                    test_bill = bill_type_bills.get(test_num)

                    if test_bill and test_bill.get('titles'):
                        for bill_title in test_bill['titles']:
                            if not bill_title:
                                continue
                            sim = calculate_title_similarity(ref['title'], bill_title)
                            if sim >= 0.8 and sim > best_sim:
                                best_sim = sim
                                best_match = {
                                    'bill_number': test_num,
                                    'title': bill_title,
                                    'confidence': sim
                                }

                # If we found a match, prepare it for insertion
                if best_match:
                    bill_id = f"{ref['bill_type']}{best_match['bill_number']}-{ref['congress_number']}"
                    matches_to_insert.append({
                        'reference_id': ref['reference_id'],
                        'run_id': run_id,
                        'match_type': 'high_confidence_match',
                        'confidence_score': best_match['confidence'],
                        'extracted_title': ref['title'],
                        'extracted_bill_number': ref['bill_number'],
                        'matched_congress': ref['congress_number'],
                        'matched_bill_type': ref['bill_type'],
                        'matched_bill_number': best_match['bill_number'],
                        'matched_title': best_match['title'],
                        'bill_id': bill_id,
                        'update_source': 'unmatched_correction'
                    })

            # Insert new matches in batches
            if matches_to_insert:
                batch_size = 100
                for i in range(0, len(matches_to_insert), batch_size):
                    batch = matches_to_insert[i:i + batch_size]
                    await conn.executemany("""
                        INSERT INTO lobbied_bill_matching.reference_matches (
                            reference_id, run_id, match_type, confidence_score,
                            extracted_title, extracted_bill_number,
                            matched_congress, matched_bill_type, matched_bill_number,
                            matched_title, bill_id, update_source
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
                        )
                        ON CONFLICT (reference_id) DO UPDATE
                        SET match_type = EXCLUDED.match_type,
                            confidence_score = EXCLUDED.confidence_score,
                            matched_congress = EXCLUDED.matched_congress,
                            matched_bill_type = EXCLUDED.matched_bill_type,
                            matched_bill_number = EXCLUDED.matched_bill_number,
                            matched_title = EXCLUDED.matched_title,
                            bill_id = EXCLUDED.bill_id,
                            update_source = EXCLUDED.update_source
                    """, [
                        (
                            m['reference_id'], m['run_id'], m['match_type'],
                            m['confidence_score'], m['extracted_title'],
                            m['extracted_bill_number'], m['matched_congress'],
                            m['matched_bill_type'], m['matched_bill_number'],
                            m['matched_title'], m['bill_id'], m['update_source']
                        )
                        for m in batch
                    ])

            logging.info(f"Updated {len(matches_to_insert)} previously unmatched references")

    except Exception as e:
        logging.error(f"Error processing unmatched references: {str(e)}", exc_info=True)
        raise

def get_single_digit_variants(number: str) -> List[str]:
    """
    Get all possible numbers that differ by a single digit.
    
    For example, for "123" returns ["023", "223", "323", ..., "120", "121", "122", "124", ...]
    excluding the original number and any numbers starting with 0.
    
    Args:
        number: Original bill number string
        
    Returns:
        List of variant number strings
    """
    variants = []
    number = str(number)
    for i in range(len(number)):
        original_digit = number[i]
        for d in '0123456789':
            if d != original_digit:
                new_number = number[:i] + d + number[i+1:]
                if not new_number.startswith('0'):
                    variants.append(new_number)
    return variants

async def post_process_wrong_titles(pool: asyncpg.Pool, run_id: int) -> None:
    """
    Check for exact matches and nearby congresses for wrong_title matches.
    
    For references marked as wrong_title, checks:
    1. The same congress for a high confidence match
    2. Adjacent congresses (+/- 1) for high confidence matches
    
    Args:
        pool: Database connection pool
        run_id: ID of the current matching run
    """
    try:
        logging.info("Starting wrong_title post-processing...")
        
        async with pool.acquire() as conn:
            # Get wrong_title matches
            wrong_titles = await conn.fetch("""
                SELECT 
                    r.reference_id,
                    r.title,
                    r.reference_type,
                    r.bill_type,
                    regexp_replace(r.bill_id, '[^0-9]', '', 'g') AS bill_number,
                    r.congress_number,
                    m.match_id,
                    m.matched_congress,
                    m.bill_id
                FROM lobbied_bill_matching.extracted_references r
                JOIN lobbied_bill_matching.reference_matches m 
                    ON r.reference_id = m.reference_id
                WHERE r.run_id = $1
                AND m.match_type = 'wrong_title'
            """, run_id)

            # Load corpus bills
            bill_trie = await load_corpus_bills(pool)
            matcher = ReferenceMatcher(bill_trie)

            updates = []
            for match in tqdm(wrong_titles, desc="Checking wrong title matches"):
                if not match['congress_number']:
                    continue

                # First check exact congress for perfect match
                reference = dict(match)
                new_match = matcher.match_reference(reference)
                if new_match and new_match.match_type == 'high_confidence_match':
                    updates.append({
                        'match_id': match['match_id'],
                        'match_type': 'high_confidence_match',
                        'matched_congress': new_match.matched_congress,
                        'matched_bill_type': new_match.matched_bill_type,
                        'matched_bill_number': new_match.matched_bill_number,
                        'bill_id': new_match.bill_id,
                        'matched_title': new_match.matched_title,
                        'confidence_score': new_match.confidence_score
                    })
                    continue

                # If no exact match, try nearby congresses
                for congress in [
                    match['congress_number'] - 1,
                    match['congress_number'] + 1
                ]:
                    reference = dict(match)
                    reference['congress_number'] = congress
                    
                    new_match = matcher.match_reference(reference)
                    if new_match and new_match.match_type == 'high_confidence_match':
                        updates.append({
                            'match_id': match['match_id'],
                            'match_type': 'high_confidence_match',
                            'matched_congress': new_match.matched_congress,
                            'matched_bill_type': new_match.matched_bill_type,
                            'matched_bill_number': new_match.matched_bill_number,
                            'bill_id': new_match.bill_id,
                            'matched_title': new_match.matched_title,
                            'confidence_score': new_match.confidence_score
                        })
                        break
            
            # Apply updates
            if updates:
                for update in updates:
                    await conn.execute("""
                        UPDATE lobbied_bill_matching.reference_matches
                        SET 
                            match_type = $1,
                            matched_congress = $2,
                            matched_bill_type = $3,
                            matched_bill_number = $4,
                            bill_id = $5,
                            matched_title = $6,
                            confidence_score = $7
                        WHERE match_id = $8
                    """,
                    update['match_type'],
                    update['matched_congress'],
                    update['matched_bill_type'],
                    update['matched_bill_number'],
                    update['bill_id'],
                    update['matched_title'],
                    update['confidence_score'],
                    update['match_id']
                    )
            
            logging.info(f"Updated {len(updates)} wrong_title matches")
            
    except Exception as e:
        logging.error(f"Error during wrong_title post-processing: {str(e)}", exc_info=True)
        raise

async def deduplicate_matches(pool: asyncpg.Pool, run_id: int) -> None:
    """
    Deduplicate matches with same bill_id within sections for a specific run.
    
    For each section, keeps only the highest confidence match for each unique bill_id,
    marking others as duplicates. Uses a batched approach with dynamic batch sizing
    to handle large datasets efficiently.
    
    Args:
        pool: Database connection pool
        run_id: ID of the current matching run
    """
    async with pool.acquire() as conn:
        try:
            # Set very long timeouts for the connection
            await conn.execute("SET statement_timeout = '12h'")  # 12 hours
            await conn.execute("SET idle_in_transaction_session_timeout = '12h'")
            
            logging.info("Checking and creating necessary indexes...")
            indexes = [
                (   
                    "idx_reference_matches_bill_id_run",
                    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_reference_matches_bill_id_run ON lobbied_bill_matching.reference_matches(bill_id, run_id) WHERE bill_id IS NOT NULL"
                ),
                (
                    "idx_reference_matches_reference_id",
                    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_reference_matches_reference_id ON lobbied_bill_matching.reference_matches(reference_id)"
                ),
                (
                    "idx_extracted_refs_section_id",
                    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_extracted_refs_section_id ON lobbied_bill_matching.extracted_references(section_id)"
                )
            ]

            for idx_name, create_stmt in indexes:
                try:
                    exists = await conn.fetchval("""
                        SELECT 1 FROM pg_indexes 
                        WHERE tablename = $1 
                        AND indexname = $2
                    """, 
                    'reference_matches' if 'reference_matches' in idx_name else 'extracted_references',
                    idx_name
                    )
                    
                    if not exists:
                        logging.info(f"Creating index {idx_name}...")
                        await conn.execute(create_stmt)
                except Exception as e:
                    logging.warning(f"Error creating index {idx_name}: {str(e)}")
                    continue  # Skip failed index but continue processing
            
            logging.info("Running deduplication...")
            
            # Instead of counting first, use a cursor-based approach with window functions
            batch_size = 50  # Start with small batch size
            total_updated = 0
            last_bill_id = None
            
            while True:
                try:
                    # Get next batch of distinct bill_ids
                    query = """
                        WITH ranked_bills AS (
                            SELECT DISTINCT m.bill_id,
                                   ROW_NUMBER() OVER (ORDER BY m.bill_id) as rn
                            FROM lobbied_bill_matching.reference_matches m
                            JOIN lobbied_bill_matching.extracted_references e 
                                ON m.reference_id = e.reference_id
                            WHERE e.run_id = $1
                            AND m.bill_id IS NOT NULL
                            AND ($2::text IS NULL OR m.bill_id > $2)
                            ORDER BY bill_id
                            LIMIT $3
                        )
                        SELECT bill_id FROM ranked_bills ORDER BY rn
                    """
                    
                    bills_batch = await conn.fetch(query, run_id, last_bill_id, batch_size)
                    
                    if not bills_batch:
                        break
                    
                    last_bill_id = bills_batch[-1]['bill_id']
                    
                    # Process each bill_id
                    for bill_record in bills_batch:
                        try:
                            # Update duplicates for this specific bill_id
                            result = await conn.execute("""
                                WITH ranked_matches AS (
                                    SELECT 
                                        m.match_id,
                                        ROW_NUMBER() OVER (
                                            PARTITION BY m.bill_id, e.section_id 
                                            ORDER BY m.confidence_score DESC, m.match_id
                                        ) as rn
                                    FROM lobbied_bill_matching.reference_matches m
                                    JOIN lobbied_bill_matching.extracted_references e 
                                        ON m.reference_id = e.reference_id
                                    WHERE e.run_id = $1
                                    AND m.bill_id = $2
                                )
                                UPDATE lobbied_bill_matching.reference_matches
                                SET match_type = 'DUPLICATE'
                                WHERE match_id IN (
                                    SELECT match_id 
                                    FROM ranked_matches 
                                    WHERE rn > 1
                                )
                            """, run_id, bill_record['bill_id'])
                            
                            if result:
                                rows = int(result.split()[-1])
                                total_updated += rows
                                
                        except Exception as e:
                            logging.warning(f"Error processing bill_id {bill_record['bill_id']}: {str(e)}")
                            continue
                        
                        await asyncio.sleep(0.01)  # Small delay between bills
                    
                    logging.info(f"Processed batch of {len(bills_batch)} bills, total updates: {total_updated}")
                    
                    # Dynamically adjust batch size based on success
                    if batch_size < 1000:  # Cap maximum batch size
                        batch_size = min(batch_size * 2, 1000)
                    
                except asyncio.TimeoutError:
                    logging.warning(f"Timeout at bill_id {last_bill_id}, reducing batch size")
                    batch_size = max(10, batch_size // 2)  # Reduce batch size but not below 10
                    await asyncio.sleep(1)  # Wait before retrying
                    continue
                    
                except Exception as e:
                    if "deadlock detected" in str(e).lower():
                        logging.warning(f"Deadlock detected at bill_id {last_bill_id}, retrying...")
                        await asyncio.sleep(1)
                        continue
                    raise
                
            logging.info(f"Deduplication complete. Marked {total_updated} matches as duplicates")
            
        except Exception as e:
            logging.error(f"Error during deduplication: {str(e)}")
            raise
        finally:
            await conn.execute("RESET statement_timeout")
            await conn.execute("RESET idle_in_transaction_session_timeout")

async def post_process_low_confidence(pool: asyncpg.Pool, run_id: int) -> None:
    """
    Check nearby congresses for high_confidence matches with low confidence scores.
    
    For matches marked as high_confidence but with low scores (<0.6), checks adjacent
    congresses (+/- 1) for better matches. This helps catch cases where the bill was
    introduced in a different congress than initially matched.
    
    Args:
        pool: Database connection pool
        run_id: ID of the current matching run
    """
    try:
        logging.info("Starting low confidence post-processing...")
        
        async with pool.acquire() as conn:
            # Get low confidence matches
            low_conf_matches = await conn.fetch("""
                SELECT 
                    r.reference_id,
                    r.title,
                    r.reference_type,
                    r.bill_type,
                    regexp_replace(r.bill_id, '[^0-9]', '', 'g') AS bill_number,
                    r.congress_number,
                    m.match_id,
                    m.matched_congress,
                    m.bill_id,
                    m.confidence_score,
                    m.update_source
                FROM lobbied_bill_matching.extracted_references r
                JOIN lobbied_bill_matching.reference_matches m 
                    ON r.reference_id = m.reference_id
                WHERE r.run_id = $1
                AND m.match_type = 'high_confidence_match'
                AND m.confidence_score < 0.6
                AND m.update_source IS NULL
            """, run_id)

            # Load corpus bills
            bill_trie = await load_corpus_bills(pool)
            matcher = ReferenceMatcher(bill_trie)

            updates = []
            for match in tqdm(low_conf_matches, desc="Checking low confidence matches"):
                if not match['congress_number']:
                    continue

                best_match = None
                best_sim = match['confidence_score']

                for congress in [
                    match['congress_number'] - 1,
                    match['congress_number'] + 1
                ]:
                    reference = dict(match)
                    reference['congress_number'] = congress
                    
                    new_match = matcher.match_reference(reference)
                    if (new_match and 
                        new_match.match_type == 'high_confidence_match' and 
                        new_match.confidence_score > best_sim):
                        best_sim = new_match.confidence_score
                        best_match = new_match

                if best_match:
                    updates.append({
                        'match_id': match['match_id'],
                        'updated_bill_id': best_match.bill_id,
                        'update_source': 'low_confidence_correction',
                        'matched_title': best_match.matched_title,
                        'confidence_score': best_match.confidence_score
                    })

            # Apply updates
            if updates:
                await conn.executemany("""
                    UPDATE lobbied_bill_matching.reference_matches
                    SET 
                        updated_bill_id = $1,
                        update_source = $2,
                        matched_title = $3,
                        confidence_score = $4
                    WHERE match_id = $5
                """, [
                    (
                        update['updated_bill_id'],
                        update['update_source'],
                        update['matched_title'],
                        update['confidence_score'],
                        update['match_id']
                    )
                    for update in updates
                ])
            
            logging.info(f"Updated {len(updates)} low confidence matches")
            
    except Exception as e:
        logging.error(f"Error during low confidence post-processing: {str(e)}", exc_info=True)
        raise

async def post_process_all(pool: asyncpg.Pool, run_id: int) -> None:
    """
    Run all post-processing steps in sequence.
    
    Executes the following steps in order:
    1. Process unmatched references
    2. Initial deduplication
    3. Post-process wrong titles
    4. Post-process low confidence matches
    5. Final deduplication
    6. Process paragraphs
    
    Each step is run with error handling and logging.
    
    Args:
        pool: Database connection pool
        run_id: ID of the current matching run
    """
    try:
        logging.info("Starting all post-processing steps...")
        
        # Define post-processing steps with descriptions
        steps = [
            (process_unmatched_refs, "Processing unmatched references"),  # Added new step
            (deduplicate_matches, "Initial deduplication"),
            (post_process_wrong_titles, "Post-processing wrong titles"),
            (post_process_low_confidence, "Post-processing low confidence matches"),
            (deduplicate_matches, "Final deduplication"),
            (process_paragraphs, "Processing paragraphs")
        ]
        
        # Run each step with progress tracking
        for step_func, description in steps:
            logging.info(f"\nStarting {description}...")
            try:
                await step_func(pool, run_id)
                logging.info(f"Completed {description}")
                await asyncio.sleep(2)  # Small delay between steps
            except Exception as e:
                logging.error(f"Error during {description}: {str(e)}")
                raise
        
        logging.info("\nAll post-processing steps completed successfully")
        
    except Exception as e:
        logging.error(f"Error during post-processing: {str(e)}", exc_info=True)
        raise