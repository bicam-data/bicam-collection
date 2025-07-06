import asyncio
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
import time
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
import logging
from tqdm.asyncio import tqdm as tqdm_asyncio
from tqdm import tqdm
import queue
import threading

from db_utils import FilingSection, DatabaseInterface
from section_processor import process_single_section_with_timeout
from timeout_handler import TimeoutTracker, TimeoutSection, RegexTimeout


def process_chunk(chunk: List[FilingSection], queue: mp.Queue) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Process a chunk of sections with enhanced logging."""
    all_results = []
    unmatched_sections = []
    
    chunk_size = len(chunk)
    logging.info(f"Starting processing of chunk with {chunk_size} sections")
    
    for idx, section in enumerate(chunk, 1):
        try:
            logging.debug(f"Processing section {section.section_id} ({idx}/{chunk_size})")
            results, unmatched = process_single_section_with_timeout(section)
            
            if results:
                all_results.extend(results)
                logging.debug(f"Added {len(results)} results from section {section.section_id}")
            if unmatched:
                unmatched_sections.extend(unmatched)
                logging.debug(f"Section {section.section_id} marked as unmatched")
                
        except RegexTimeout as e:
            logging.warning(f"Timeout processing section {section.section_id}")
            queue.put({
                'filing_uuid': section.filing_uuid,
                'section_id': section.section_id,
                'pattern_type': 'section',
                'text_length': len(section.text),
                'error': str(e)
            })
        except Exception as e:
            logging.error(f"Error processing section {section.section_id}: {str(e)}", exc_info=True)
    
    logging.info(f"Chunk complete: {len(all_results)} total matches, {len(unmatched_sections)} unmatched sections")
    return all_results, unmatched_sections

@dataclass
class BatchProcessor:
    """Handles parallel processing of section batches."""
    db: DatabaseInterface
    batch_size: int = 1000
    max_concurrent_batches: int = 4
    max_workers_per_batch: int = max(2, mp.cpu_count() // 4)
    timeout_tracker: Optional[TimeoutTracker] = None
    
    async def process_all_sections(self, run_id: int, sections: Optional[List] = None) -> List[Dict[str, Any]]:
        """Process all sections in parallel batches with optimized resource usage."""
        # Handle sections input
        if sections is not None:
            sections_list = []
            for section in sections:
                if isinstance(section, FilingSection):
                    sections_list.append(section)
                else:
                    # Handle raw database rows if they come in that format
                    sections_list.append(FilingSection(
                        filing_uuid=str(section['filing_uuid']),
                        section_id=str(section['section_id']),
                        text=section['issue_text'] if section['issue_text'] else '',
                        filing_year=section['filing_year']
                    ))
        else:
            # Fetch sections if none provided
            sections_list = []
            async with self.db.pool.acquire() as conn:
                async with conn.transaction():
                    rows = await conn.fetch("""
                        SELECT 
                            fs.filing_uuid,
                            fs.section_id,
                            COALESCE(fst.issue_text, '') as text,
                            f.filing_year
                        FROM relational___lda.filing_sections fs
                        JOIN relational___lda.filing_sections_text fst 
                            ON fs.section_id = fst.section_id
                        JOIN relational___lda.filings f 
                            ON fs.filing_uuid = f.filing_uuid
                        WHERE fst.issue_text IS NOT NULL
                        AND length(fst.issue_text) > 3
                        ORDER BY fs.section_id
                    """)
                    for row in rows:
                        sections_list.append(FilingSection(
                            filing_uuid=str(row['filing_uuid']),
                            section_id=row['section_id'],
                            text=row['text'],
                            filing_year=row['filing_year']
                        ))
        
        total_sections = len(sections_list)
        logging.info(f"Processing {total_sections} sections")
        
        # Create reasonable-sized batches
        batches = [
            sections_list[i:i + self.batch_size]
            for i in range(0, len(sections_list), self.batch_size)
        ]
        
        # Set up multiprocessing resources
        ctx = mp.get_context('spawn')
        timeout_queue = ctx.Queue()
        
        # Process batches with controlled concurrency
        all_results = []
        batch_semaphore = asyncio.Semaphore(self.max_concurrent_batches)
        
        async def process_batch(batch: List[FilingSection], batch_idx: int) -> List[Dict[str, Any]]:
            async with batch_semaphore:
                try:
                    batch_start_time = time.time()
                    logging.info(f"Starting batch {batch_idx + 1}/{len(batches)} ({len(batch)} sections)")
                    
                    results = []
                    unmatched_sections = []
                    
                    # Create a Manager for this batch
                    with mp.Manager() as manager:
                        # Create a queue using the manager
                        batch_queue = manager.Queue()
                        
                        # Process in chunks
                        with ProcessPoolExecutor(max_workers=self.max_workers_per_batch) as executor:
                            chunk_size = max(10, len(batch) // self.max_workers_per_batch)
                            chunks = [batch[i:i + chunk_size] for i in range(0, len(batch), chunk_size)]
                            logging.debug(f"Batch {batch_idx}: Created {len(chunks)} chunks of size ~{chunk_size}")
                            
                            chunk_futures = []
                            for chunk_idx, chunk in enumerate(chunks):
                                future = executor.submit(process_chunk, chunk, batch_queue)
                                chunk_futures.append((chunk_idx, future))
                            
                            for chunk_idx, future in chunk_futures:
                                try:
                                    chunk_results, chunk_unmatched = future.result(timeout=300)
                                    results.extend(chunk_results)
                                    unmatched_sections.extend(chunk_unmatched)
                                    logging.debug(f"Batch {batch_idx}, Chunk {chunk_idx}: {len(chunk_results)} matches")
                                except Exception as e:
                                    logging.error(f"Error in batch {batch_idx}, chunk {chunk_idx}: {str(e)}")
                                    
                            # Process any timeouts from the batch queue
                            while True:
                                try:
                                    item = batch_queue.get_nowait()
                                    if item and self.timeout_tracker:
                                        timeout = TimeoutSection(**item)
                                        self.timeout_tracker.add_timeout(timeout)
                                except queue.Empty:
                                    break
                                except Exception as e:
                                    logging.error(f"Error processing timeout: {str(e)}")

                    # Store results
                    if results:
                        logging.info(f"Batch {batch_idx}: Storing {len(results)} results")
                        async with self.db.pool.acquire() as conn:
                            async with conn.transaction():
                                await self.db.bulk_insert_references(conn, run_id, results)
                    
                    if unmatched_sections:
                        logging.info(f"Batch {batch_idx}: Storing {len(unmatched_sections)} unmatched sections")
                        async with self.db.pool.acquire() as conn:
                            async with conn.transaction():
                                await self.db.insert_unmatched_sections(conn, run_id, unmatched_sections)
                    
                    batch_time = time.time() - batch_start_time
                    logging.info(f"Batch {batch_idx} complete in {batch_time:.1f}s: {len(results)} matches, " +
                            f"{len(unmatched_sections)} unmatched")
                    
                    return results
                    
                except Exception as e:
                    logging.error(f"Batch {batch_idx} failed: {str(e)}", exc_info=True)
                    return []

        # Process timeouts
        def handle_timeouts():
            while True:
                try:
                    timeout_info = timeout_queue.get(timeout=1.0)
                    if timeout_info is None:
                        break
                    if self.timeout_tracker:
                        timeout = TimeoutSection(**timeout_info)
                        self.timeout_tracker.add_timeout(timeout)
                except queue.Empty:
                    continue
                except Exception as e:
                    logging.error(f"Timeout handling error: {str(e)}")
        
        # Start timeout handler thread
        timeout_thread = threading.Thread(target=handle_timeouts)
        timeout_thread.daemon = True
        timeout_thread.start()
        
        try:
            # Process all batches
            batch_tasks = [
                process_batch(batch, idx) 
                for idx, batch in enumerate(batches)
            ]
            
            # Collect results as batches complete
            with tqdm(total=len(batches), desc="Processing batches") as pbar:
                for future in asyncio.as_completed(batch_tasks):
                    try:
                        batch_results = await future
                        if batch_results:
                            all_results.extend(batch_results)
                        pbar.update(1)
                    except Exception as e:
                        logging.error(f"Batch processing error: {str(e)}")
                        continue
        
        finally:
            # Clean up
            timeout_queue.put(None)
            timeout_thread.join(timeout=5.0)
            timeout_queue.close()
            
            if self.timeout_tracker:
                await self.timeout_tracker.store_all_timeouts(run_id)
        
        return all_results

async def process_sections_in_parallel(
    db: DatabaseInterface,
    run_id: int,
    batch_size: int = 1000,
    max_concurrent_batches: int = 4,
    max_workers_per_batch: Optional[int] = None,
    timeout_tracker: Optional[TimeoutTracker] = None
) -> List[Dict[str, Any]]:
    """Main entry point for parallel section processing."""
    processor = BatchProcessor(
        db=db,
        batch_size=batch_size,
        max_concurrent_batches=max_concurrent_batches,
        max_workers_per_batch=max_workers_per_batch or (mp.cpu_count() // 4),
        timeout_tracker=timeout_tracker
    )
    
    return await processor.process_all_sections(run_id)

