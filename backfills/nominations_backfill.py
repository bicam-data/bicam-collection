import asyncio
import logging
from typing import Any, Dict, Optional, List
from datetime import datetime
from dotenv import load_dotenv
import os

from base_backfill import APIBackfillProcessor

class CongressNominationBackfill(APIBackfillProcessor):
    def __init__(self, input_csv_path: str):
        load_dotenv('.env.gov')
        
        api_keys = [
            value for key, value in os.environ.items() 
            if key.startswith("CONGRESS_API_KEY")
        ]
        
        super().__init__(
            input_csv_path=input_csv_path,
            data_type='nominations',
            api_type='congress',
            api_keys=api_keys,
            required_columns=['congress', 'nomination_number']
        )

    def get_cache_key(self, row: Dict[str, str]) -> str:
        """Get cache key for a nomination"""
        return f"{row['congress']}_{row['nomination_number']}"

    async def fetch_item(self, row: Dict[str, str]) -> Optional[Any]:
        """Fetch nomination from API"""
        self.logger.info(f"[FETCH] Fetching nomination {row['nomination_number']} for congress {row['congress']}")
        return await self.api_client.get_nomination(
            congress=int(row['congress']),
            nomination_number=row['nomination_number']
        )

    async def process_item(self, api_item: Optional[Any] = None, row: Optional[Dict[str, str]] = None) -> Optional[Dict]:
        """Process nomination data using cached API object"""
        if not api_item:
            return None
            
        try:
            return {
                'nomination_id': api_item.nomination_id,
                'congress': api_item.congress,
                'nomination_number': api_item.nomination_number,
                'part_number': api_item.part_number,
                'description': api_item.description,
                'received_at': api_item.received_at,
                'authority_date': api_item.authority_date,
                'is_privileged': api_item.is_privileged,
                'is_civilian': api_item.is_civilian,
                'executive_calendar_number': api_item.executive_calendar_number,
                'committees_count': api_item.committees_count,
                'actions_count': api_item.actions_count,
                'hearings_count': api_item.hearings_count,
                'updated_at': api_item.updated_at
            }
        except Exception as e:
            identifier = f"{row['congress']}_{row['nomination_number']}" if row else 'unknown'
            self.logger.error(f"Error processing nomination {identifier}: {str(e)}")
            return None

    async def process_nested_data(self, item: Dict, api_item: Optional[Any] = None) -> Dict[str, List[Dict]]:
        """Process nested fields using cached nomination object"""
        results = {}
        
        if not api_item:
            return results

        try:
            # Process nominee positions
            if hasattr(api_item, 'nomineepositions'):
                results['nomineepositions'] = [{
                    'nomination_id': item['nomination_id'],
                    'ordinal': pos.ordinal,
                    'position_title': pos.position_title,
                    'organization': pos.organization,
                    'intro_text': pos.intro_text,
                    'nominee_count': pos.nominee_count
                } for pos in api_item.nomineepositions]

        except Exception as e:
            self.logger.error(f"Error processing nested data for nomination {item['nomination_id']}: {str(e)}")

        return results

    async def process_related_data(self, item: Dict, api_item: Optional[Any] = None) -> Dict[str, List[Dict]]:
        """Process related data using cached nomination object"""
        results = {}
        
        if not api_item:
            return results

        try:
            if hasattr(api_item, 'get_actions'):
                actions_list = []
                actions_committee_codes_list = []
                async for action in api_item.get_actions():
                    # Main action data - ONLY fields that exist in nominations_actions table
                    action_data = {
                        'nomination_id': item['nomination_id'],
                        'action_id': action.action_id,
                        'action_code': action.action_code,
                        'action_date': action.action_date,
                        'text': action.text,
                        'action_type': action.action_type
                    }
                    actions_list.append(action_data)

                    # Store committee codes separately for nominations_actions_committee_codes table
                    if hasattr(action, 'committee_codes') and action.committee_codes:
                        for committee_code in action.committee_codes:
                            actions_committee_codes_list.append({
                                'nomination_id': item['nomination_id'],
                                'action_id': action.action_id,
                                'committee_codes': committee_code
                            })

                if actions_list:
                    results['actions'] = actions_list
                if actions_committee_codes_list:
                    results['actions_committee_codes'] = actions_committee_codes_list
            # Process committee activities
            if hasattr(api_item, 'get_committeeactivities'):
                activities_list = []
                async for activity in api_item.get_committeeactivities():
                    activities_list.append({
                        'nomination_id': item['nomination_id'],
                        'activity_date': activity.activity_date,
                        'activity_name': activity.activity_name,
                        'committee': activity.committee,
                        'committee_code': activity.committee_code
                    })
                results['committeeactivities'] = activities_list

            # First get positions to use their ordinals
            positions = []
            if hasattr(api_item, 'nomineepositions'):
                positions = api_item.nomineepositions

            # Process nominees with corresponding position ordinals
            if hasattr(api_item, 'get_nominees') and positions:
                nominees_list = []
                position_idx = 0  # Track position index for ordinal
                
                async for nominee in api_item.get_nominees():
                    # Make sure we don't exceed positions
                    if position_idx < len(positions):
                        position = positions[position_idx]
                        nominees_list.append({
                            'nomination_id': item['nomination_id'],
                            'ordinal': position.ordinal,  # Use position's ordinal
                            'first_name': nominee.first_name,
                            'last_name': nominee.last_name,
                            'middle_name': nominee.middle_name,
                            'prefix': nominee.prefix,
                            'suffix': nominee.suffix,
                            'state': nominee.state,
                            'effective_date': nominee.effective_date,
                            'predecessor_name': nominee.predecessor_name,
                            'corps_code': nominee.corps_code
                        })
                        position_idx += 1  # Move to next position
                
                if nominees_list:
                    results['nominees'] = nominees_list
            # Process hearings
            if hasattr(api_item, 'get_hearings'):
                hearings_list = []
                async for hearing in api_item.get_hearings():
                    hearings_list.append({
                        'nomination_id': item['nomination_id'],
                        'hearing_jacketnumber': hearing.hearing_jacketnumber,
                        'part_number': hearing.part_number,
                        'errata_number': hearing.errata_number
                    })
                results['hearings'] = hearings_list

        except Exception as e:
            self.logger.error(f"Error processing related data for nomination {item['nomination_id']}: {str(e)}")

        return results

async def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    import argparse
    parser = argparse.ArgumentParser(description="Backfill nominations data from Congress API")
    args = parser.parse_args()
    
    processor = CongressNominationBackfill(input_csv_path='nomination_ordinals_to_backfill.csv')
    await processor.run(batch_size=5)

if __name__ == "__main__":
    asyncio.run(main())