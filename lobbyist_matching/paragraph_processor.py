"""
Module for processing sections into paragraphs and matching bills.

This module handles splitting sections into paragraphs and associating bill matches
with the appropriate paragraphs. It provides functionality for:

- Splitting section text into logical paragraphs
- Storing paragraphs in the database
- Matching bills to paragraphs based on text position
- Handling duplicate paragraphs
"""

import logging
from typing import List
import asyncpg
from tqdm import tqdm
import re

async def process_paragraphs(pool: asyncpg.Pool, run_id: int) -> None:
    """Process sections into paragraphs and match bills.
    
    This function:
    1. Clears existing paragraph data for the run
    2. Gets sections and their associated bill matches
    3. Splits each section into paragraphs
    4. Stores paragraphs in the database
    5. Associates bill matches with paragraphs based on text position

    Args:
        pool (asyncpg.Pool): Database connection pool
        run_id (int): ID of the current processing run

    Raises:
        Exception: If there is an error during database operations
    """
    async with pool.acquire() as conn:
        # Clear existing paragraphs - execute DELETE statements separately
        await conn.execute("""
            DELETE FROM lobbied_bill_matching.paragraph_matches 
            WHERE run_id = $1
        """, run_id)
        
        await conn.execute("""
            DELETE FROM lobbied_bill_matching.section_paragraphs 
            WHERE run_id = $1
        """, run_id)
        
        # Get sections and their bills
        sections = await conn.fetch("""
            SELECT DISTINCT 
                fs.filing_uuid,
                fs.section_id::INTEGER as section_id,
                fst.issue_text as text,
                array_agg(DISTINCT m.match_id) as match_ids,
                array_agg(DISTINCT e.start_position) as start_positions,
                array_agg(DISTINCT e.end_position) as end_positions
            FROM lobbied_bill_matching.reference_matches m
            JOIN lobbied_bill_matching.extracted_references e 
                ON m.reference_id = e.reference_id
            JOIN relational___lda.filing_sections fs 
                ON e.section_id = fs.section_id::TEXT
            JOIN relational___lda.filing_sections_text fst 
                ON fs.section_id = fst.section_id
            WHERE e.run_id = $1
            GROUP BY fs.filing_uuid, fs.section_id, fst.issue_text
        """, run_id)
        
        for section in tqdm(sections, desc="Processing sections"):
            paragraphs = split_into_paragraphs(section['text'])
            
            # Store paragraphs and match bills
            current_pos = 0
            for i, para in enumerate(paragraphs, 1):
                start_pos = section['text'].find(para, current_pos)
                if start_pos == -1:
                    continue
                
                end_pos = start_pos + len(para)
                current_pos = end_pos
                
                # Store paragraph
                paragraph_id = await conn.fetchval("""
                    INSERT INTO lobbied_bill_matching.section_paragraphs (
                        run_id, section_id, paragraph_text, 
                        paragraph_ordinal, start_position, end_position
                    ) VALUES ($1, $2, $3, $4, $5, $6)
                    RETURNING paragraph_id
                """, run_id, section['section_id'], para, i, start_pos, end_pos)
                
                if paragraph_id is None:
                    logging.warning(f"Failed to insert paragraph for section {section['section_id']}")
                    continue
                
                # Match bills to paragraph
                if section['match_ids'] and section['start_positions'] and section['end_positions']:
                    for match_id, bill_start, bill_end in zip(
                        section['match_ids'],
                        section['start_positions'],
                        section['end_positions']
                    ):
                        if match_id is not None and start_pos <= bill_start and bill_end <= end_pos:
                            try:
                                await conn.execute("""
                                    INSERT INTO lobbied_bill_matching.paragraph_matches (
                                        paragraph_id, match_id, run_id
                                    ) VALUES ($1, $2, $3)
                                    ON CONFLICT DO NOTHING
                                """, paragraph_id, match_id, run_id)
                            except Exception as e:
                                logging.error(f"Error inserting paragraph match: {str(e)}")

def split_into_paragraphs(text: str) -> List[str]:
    """Split text into logical paragraphs.
    
    This function splits text into paragraphs using the following rules:
    1. Split on double newlines
    2. Further split on single newlines and semicolons
    3. Clean up whitespace
    4. Remove duplicate paragraphs while preserving order
    
    Args:
        text (str): The text to split into paragraphs
        
    Returns:
        List[str]: List of unique paragraphs in order of appearance
        
    Examples:
        >>> text = "Para 1\\n\\nPara 2; Para 3\\nPara 4"
        >>> split_into_paragraphs(text)
        ['Para 1', 'Para 2', 'Para 3', 'Para 4']
    """
    if not text:
        return []
    
    # Normalize newlines
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    
    # Split on paragraph breaks
    splits = text.split('\n\n')
    paragraphs = []
    
    for split in splits:
        # Further split on newlines and semicolons
        subsplits = re.split(r'(?:\n|;\s+)', split)
        
        for subsplit in subsplits:
            cleaned = ' '.join(subsplit.split())
            if cleaned and not cleaned.isspace():
                paragraphs.append(cleaned)
    
    # Handle single paragraph case
    if not paragraphs and text.strip():
        paragraphs = [' '.join(text.split())]
    
    # Remove duplicates while preserving order
    seen = set()
    return [p for p in paragraphs if not (p in seen or seen.add(p))]