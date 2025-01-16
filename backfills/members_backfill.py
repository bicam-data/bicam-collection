import asyncio
import logging
from typing import Any, Dict, Optional, List
from datetime import datetime
from dotenv import load_dotenv
import os

from base_backfill import APIBackfillProcessor

class CongressMemberBackfill(APIBackfillProcessor):
    def __init__(self, input_csv_path: str):
        load_dotenv('.env.gov')
        
        api_keys = [
            value for key, value in os.environ.items() 
            if key.startswith("CONGRESS_API_KEY")
        ]
        
        super().__init__(
            input_csv_path=input_csv_path,
            data_type='members',
            api_type='congress',
            api_keys=api_keys,
            required_columns=['bioguide_id']
        )

    def get_cache_key(self, row: Dict[str, str]) -> str:
        """Get cache key for a member"""
        return row['bioguide_id']

    async def fetch_item(self, row: Dict[str, str]) -> Optional[Any]:
        """Fetch member from API"""
        self.logger.info(f"[FETCH] Fetching member {row['bioguide_id']}")
        return await self.api_client.get_member(
            bioguide_id=row['bioguide_id']
        )

    async def process_item(self, api_item: Optional[Any] = None, row: Optional[Dict[str, str]] = None) -> Optional[Dict]:
        """Process member data using cached API object"""
        if not api_item:
            return None
            
        try:
            return {
                'bioguide_id': api_item.bioguide_id,
                'direct_order_name': api_item.direct_order_name,
                'inverted_order_name': api_item.inverted_order_name,
                'honorific_name': api_item.honorific_name,
                'first_name': api_item.first_name,
                'middle_name': api_item.middle_name,
                'last_name': api_item.last_name,
                'suffix_name': api_item.suffix_name,
                'nickname': api_item.nickname,
                'party': api_item.party,
                'state': api_item.state,
                'district': api_item.district,
                'birth_year': api_item.birth_year,
                'death_year': api_item.death_year,
                'official_url': api_item.official_url,
                'office_address': api_item.office_address,
                'office_city': api_item.office_city,
                'office_district': api_item.office_district,
                'office_zip': api_item.office_zip,
                'office_phone_number': api_item.office_phone_number,
                'sponsored_legislation_count': api_item.sponsored_legislation_count,
                'cosponsored_legislation_count': api_item.cosponsored_legislation_count,
                'depiction_image_url': api_item.depiction_image_url,
                'depiction_attribution': api_item.depiction_attribution,
                'is_current_member': api_item.is_current_member,
                'updated_at': api_item.updated_at
            }
        except Exception as e:
            identifier = row['bioguide_id'] if row else 'unknown'
            self.logger.error(f"Error processing member {identifier}: {str(e)}")
            return None

    async def process_nested_data(self, item: Dict, api_item: Optional[Any] = None) -> Dict[str, List[Dict]]:
        """Process nested fields using cached member object"""
        results = {}
        
        if not api_item:
            return results

        try:
            # Process terms
            if hasattr(api_item, 'terms'):
                results['terms'] = [{
                    'bioguide_id': item['bioguide_id'],
                    'chamber': term.chamber,
                    'congress': term.congress,
                    'state_code': term.state_code,
                    'state_name': term.state_name,
                    'district': term.district,
                    'start_year': term.start_year,
                    'end_year': term.end_year,
                    'member_type': term.member_type
                } for term in api_item.terms]

            # Process leadership roles
            if hasattr(api_item, 'leadership_roles'):
                results['leadership_roles'] = [{
                    'bioguide_id': item['bioguide_id'],
                    'congress': role.congress,
                    'chamber': role.chamber,
                    'type': role.type,
                    'is_current': role.is_current
                } for role in api_item.leadership_roles]

            # Process party history
            if hasattr(api_item, 'party_history'):
                results['party_history'] = [{
                    'bioguide_id': item['bioguide_id'],
                    'party_code': history.party_code,
                    'party_name': history.party_name,
                    'start_year': history.start_year,
                    'end_year': history.end_year
                } for history in api_item.party_history]

        except Exception as e:
            self.logger.error(f"Error processing nested data for member {item['bioguide_id']}: {str(e)}")

        return results

    async def process_related_data(self, item: Dict, api_item: Optional[Any] = None) -> Dict[str, List[Dict]]:
        """Process related data using cached member object"""
        results = {}
        
        if not api_item:
            return results

        try:
            # Additional related data processing if needed in the future
            pass

        except Exception as e:
            self.logger.error(f"Error processing related data for member {item['bioguide_id']}: {str(e)}")

        return results

async def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    import argparse
    parser = argparse.ArgumentParser(description="Backfill members data from Congress API")
    args = parser.parse_args()
    
    processor = CongressMemberBackfill(input_csv_path='members_to_backfill.csv')
    await processor.run(batch_size=5)

if __name__ == "__main__":
    asyncio.run(main())