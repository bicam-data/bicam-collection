import asyncio
import logging
from typing import Any, Dict, Optional, List
from datetime import datetime
from dotenv import load_dotenv
import os

from base_backfill import APIBackfillProcessor

class CongressCommitteeBackfill(APIBackfillProcessor):
    def __init__(self, input_csv_path: str):
        load_dotenv('.env.gov')
        
        api_keys = [
            value for key, value in os.environ.items() 
            if key.startswith("CONGRESS_API_KEY")
        ]
        
        super().__init__(
            input_csv_path=input_csv_path,
            data_type='committees',
            api_type='congress',
            api_keys=api_keys,
            required_columns=['committee_code', 'chamber']
        )

    def get_cache_key(self, row: Dict[str, str]) -> str:
        """Get cache key for a committee"""
        return f"{row['committee_code']}"

    async def fetch_item(self, row: Dict[str, str]) -> Optional[Any]:
        """Fetch committee from API"""
        self.logger.info(f"[FETCH] Fetching committee {row['committee_code']} for {row['chamber']}")
        return await self.api_client.get_committee(
            chamber=row['chamber'].lower(),
            committee_code=row['committee_code']
        )

    async def process_item(self, api_item: Optional[Any] = None, row: Optional[Dict[str, str]] = None) -> Optional[Dict]:
        """Process committee data using cached API object"""
        if not api_item:
            return None
            
        try:
            return {
                'committee_code': api_item.committee_code,
                'committee_type': api_item.committee_type,
                'chamber': api_item.chamber,
                'name': api_item.name,
                'is_subcommittee': api_item.is_subcommittee,
                'is_current': api_item.is_current,
                'bills_count': api_item.bills_count,
                'reports_count': api_item.reports_count,
                'nominations_count': api_item.nominations_count,
                'updated_at': api_item.updated_at
            }
        except Exception as e:
            identifier = row['committee_code'] if row else 'unknown'
            self.logger.error(f"Error processing committee {identifier}: {str(e)}")
            return None

    async def process_nested_data(self, item: Dict, api_item: Optional[Any] = None) -> Dict[str, List[Dict]]:
        """Process nested fields using cached committee object"""
        results = {}
        
        if not api_item:
            return results

        try:
            # Process history
            if hasattr(api_item, 'history'):
                history_obj = api_item.history
                results['history'] = [{
                    'committee_code': item['committee_code'],
                    'official_name': hist.official_name,
                    'loc_name': hist.loc_name,
                    'start_date': hist.start_date,
                    'end_date': hist.end_date,
                    'committee_type_code': hist.committee_type_code,
                    'establishing_authority': hist.establishing_authority,
                    'superintendent_document_number': hist.superintendent_document_number,
                    'nara_id': hist.nara_id,
                    'loc_linked_data_id': hist.loc_linked_data_id,
                    'updated_at': hist.updated_at
                } for hist in history_obj]

            # Process subcommittees
            if hasattr(api_item, 'subcommittees_codes'):
                results['subcommittees'] = [{
                    'committee_code': item['committee_code'],
                    'subcommittee_code': code
                } for code in api_item.subcommittees_codes]

        except Exception as e:
            self.logger.error(f"Error processing nested data for committee {item['committee_code']}: {str(e)}")

        return results

    async def process_related_data(self, item: Dict, api_item: Optional[Any] = None) -> Dict[str, List[Dict]]:
        """Process related data using cached committee object"""
        results = {}
        
        if not api_item:
            return results

        try:
            # Process bills
            if hasattr(api_item, 'get_bills'):
                bills_list = []
                async for bill in api_item.get_bills():
                    bills_list.append({
                        'committee_code': item['committee_code'],
                        'bill_id': bill.bill_id,
                        'relationship_type': bill.relationship_type,
                        'committee_action_date': bill.committee_action_date,
                        'updated_at': bill.updated_at
                    })
                results['bills'] = bills_list

            # Process associated reports
            if hasattr(api_item, 'get_associated_reports'):
                reports_list = []
                async for report in api_item.get_associated_reports():
                    reports_list.append({
                        'committee_code': item['committee_code'],
                        'report_id': report.report_id
                    })
                results['committeereports'] = reports_list

        except Exception as e:
            self.logger.error(f"Error processing related data for committee {item['committee_code']}: {str(e)}")

        return results

    def get_unique_id(self, data: Dict) -> str:
        """Get unique identifier for a committee"""
        return data['committee_code']

async def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    processor = CongressCommitteeBackfill(
        input_csv_path='committees_to_backfill.csv'
    )
    
    await processor.run(batch_size=5)

if __name__ == "__main__":
    asyncio.run(main())