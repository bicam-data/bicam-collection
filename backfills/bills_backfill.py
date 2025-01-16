import asyncio
import logging
from typing import Any, Dict, Optional, List
from datetime import datetime
from dotenv import load_dotenv
import os

from base_backfill import APIBackfillProcessor

class CongressBillBackfill(APIBackfillProcessor):
    def __init__(self, input_csv_path: str):
        load_dotenv('.env.gov')
        
        api_keys = [
            value for key, value in os.environ.items() 
            if key.startswith("CONGRESS_API_KEY")
        ]
        
        super().__init__(
            input_csv_path=input_csv_path,
            data_type='bills',
            api_type='congress',
            api_keys=api_keys,
            required_columns=['congress', 'bill_type', 'bill_number']
        )

    def get_cache_key(self, row: Dict[str, str]) -> str:
        """Get cache key for a bill"""
        return f"{row['congress']}_{row['bill_type']}_{row['bill_number']}"

    async def fetch_item(self, row: Dict[str, str]) -> Optional[Any]:
        """Fetch bill from API"""
        self.logger.info(f"[FETCH] Fetching bill {row['bill_type']}{row['bill_number']} for congress {row['congress']}")
        return await self.api_client.get_bill(
            congress=int(row['congress']),
            bill_type=row['bill_type'],
	    bill_number=int(row['bill_number'])
        )

    async def process_item(self, api_item: Optional[Any] = None, row: Optional[Dict[str, str]] = None) -> Optional[Dict]:
        """Process bill data using cached API object"""
        if not api_item:
            return None
            
        try:
            return {
                'bill_id': api_item.bill_id,
                'bill_type': api_item.bill_type,
                'bill_number': api_item.bill_number,
                'congress': api_item.congress,
                'title': api_item.title,
                'origin_chamber': api_item.origin_chamber,
                'origin_chamber_code': api_item.origin_chamber_code,
                'introduced_at': api_item.introduced_at,
                'constitutional_authority_statement_text': api_item.constitutional_authority_statement_text,
                'is_law': api_item.is_law,
                'notes': api_item.notes,
                'policy_area': api_item.policy_area,
                'actions_count': api_item.actions_count,
                'amendments_count': api_item.amendments_count,
                'committees_count': api_item.committees_count,
                'cosponsors_count': api_item.cosponsors_count,
                'bill_relations_count': api_item.billrelations_count,
                'summaries_count': api_item.summaries_count,
                'subjects_count': api_item.subjects_count,
                'titles_count': api_item.titles_count,
                'texts_count': api_item.texts_count,
                'updated_at': api_item.updated_at
            }
        except Exception as e:
            identifier = f"{row['congress']}_{row['bill_type']}{row['bill_number']}" if row else 'unknown'
            self.logger.error(f"Error processing bill {identifier}: {str(e)}")
            return None

    async def process_nested_data(self, item: Dict, api_item: Optional[Any] = None) -> Dict[str, List[Dict]]:
        """Process nested fields using cached bill object"""
        results = {}
        
        if not api_item:
            return results

        try:
            # Process CBO cost estimates
            if hasattr(api_item, 'cbocostestimates'):
                results['cbocostestimates'] = [{
                    'bill_id': item['bill_id'],
                    'url': estimate.url,
                    'pub_date': estimate.pub_date,
                    'title': estimate.title,
                    'description': estimate.description
                } for estimate in api_item.cbocostestimates]

            # Process sponsors
            if hasattr(api_item, 'sponsors'):
                results['sponsors'] = [{
                    'bill_id': item['bill_id'],
                    'sponsors': sponsor
                } for sponsor in api_item.sponsors]

            # Process laws
            if hasattr(api_item, 'laws'):
                results['laws'] = [{
                    'bill_id': item['bill_id'],
                    'law_number': law.law_number,
                    'law_type': law.law_type
                } for law in api_item.laws]

        except Exception as e:
            self.logger.error(f"Error processing nested data for bill {item['bill_id']}: {str(e)}")

        return results

    async def process_related_data(self, item: Dict, api_item: Optional[Any] = None) -> Dict[str, List[Dict]]:
        """Process related data using cached bill object"""
        results = {}
        
        if not api_item:
            return results

        try:
            # Process actions
            if hasattr(api_item, 'get_actions'):
                actions_list = []
                actions_committee_codes_list = []
                actions_recorded_votes_list = []
                
                async for action in api_item.get_actions():
                    action_data = {
                        'bill_id': item['bill_id'],
                        'action_id': action.action_id,
                        'action_code': action.action_code,
                        'action_date': action.action_date,
                        'text': action.text,
                        'action_type': action.action_type,
                        'source_system': action.source_system,
                        'source_system_code': action.source_system_code,
                        'calendar': action.calendar,
                        'calendar_number': action.calendar_number
                    }
                    actions_list.append(action_data)

                    # Process committee codes for action
                    if hasattr(action, 'committee_codes') and action.committee_codes:
                        for committee_codes in action.committee_codes:
                            actions_committee_codes_list.append({
                                'bill_id': item['bill_id'],
                                'action_id': action.action_id,
                                'committee_codes': committee_codes
                            })

                    # Process recorded votes for action
                    if hasattr(action, 'recorded_votes') and action.recorded_votes:
                        for vote in action.recorded_votes:
                            actions_recorded_votes_list.append({
                                'bill_id': item['bill_id'],
                                'action_id': action.action_id,
                                'roll_number': vote.roll_number,
                                'chamber': vote.chamber,
                                'congress': vote.congress,
                                'session_number': vote.session_number,
                                'date': vote.date,
                                'url': vote.url
                            })

                if actions_list:
                    results['actions'] = actions_list
                if actions_committee_codes_list:
                    results['actions_committee_codes'] = actions_committee_codes_list
                if actions_recorded_votes_list:
                    results['actions_recorded_votes'] = actions_recorded_votes_list

            # Process cosponsors
            if hasattr(api_item, 'get_cosponsors'):
                cosponsors_list = []
                async for cosponsor in api_item.get_cosponsors():
                    cosponsors_list.append({
                        'bill_id': item['bill_id'],
                        'bioguide_id': cosponsor.bioguide_id
                    })
                if cosponsors_list:
                    results['cosponsors'] = cosponsors_list

            # Process bill relations
            if hasattr(api_item, 'get_billrelations'):
                relations_list = []
                async for relation in api_item.get_billrelations():
                    relations_list.append({
                        'bill_id': item['bill_id'],
                        'relatedbill_id': relation.relatedbill_id,
                        'relationship_identified_by_1': relation.relationship_identified_by_1,
                        'relationship_type_1': None,
                        'relationship_identified_by_2': relation.relationship_identified_by_2,
                        'relationship_type_2': None,
                        'relationship_identified_by_3': relation.relationship_identified_by_3,
                        'relationship_type_3': None
                    })
                if relations_list:
                    results['billrelations'] = relations_list

            # Process texts
            if hasattr(api_item, 'get_texts'):
                texts_list = []
                async for text in api_item.get_texts():
                    texts_list.append({
                        'bill_id': item['bill_id'],
                        'description': text.description,
                        'date': text.date,
                        'type': text.type,
                        'formatted_text': text.formatted_text,
                        'pdf': text.pdf,
                        'html': text.html,
                        'xml': text.xml,
                        'url': text.url
                    })
                if texts_list:  
                    results['texts'] = texts_list

            # Process titles
            if hasattr(api_item, 'get_titles'):
                titles_list = []
                async for title in api_item.get_titles():
                    titles_list.append({
                        'bill_id': item['bill_id'],
                        'title': title.title,
                        'title_type': title.title_type,
                        'bill_text_version_code': title.bill_text_version_code,
                        'bill_text_version_name': title.bill_text_version_name,
                        'chamber_code': title.chamber_code,
                        'chamber': title.chamber,
                        'title_type_code': title.title_type_code
                    })
                if titles_list:
                    results['titles'] = titles_list

            # Process summaries
            if hasattr(api_item, 'get_summaries'):
                summaries_list = []
                async for summary in api_item.get_summaries():
                    summaries_list.append({
                        'bill_id': item['bill_id'],
                        'action_date': summary.action_date,
                        'action_desc': summary.action_desc,
                        'text': summary.text,
                        'version_code': summary.version_code,
                        'updated_at': summary.updated_at
                    })
                if summaries_list:
                    results['summaries'] = summaries_list

            # Process subjects
            if hasattr(api_item, 'get_subjects'):
                subjects_list = []
                async for subject in api_item.get_subjects():
                    subjects_list.append({
                        'bill_id': item['bill_id'],
                        'subject': subject.subject,
                        'updated_at': subject.updated_at
                    })
                if subjects_list:
                    results['subjects'] = subjects_list

        except Exception as e:
            self.logger.error(f"Error processing related data for bill {item['bill_id']}: {str(e)}")

        return results

async def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    import argparse
    parser = argparse.ArgumentParser(description="Backfill bills data from Congress API")
    args = parser.parse_args()
    
    processor = CongressBillBackfill(input_csv_path='bills_hearings_to_backfill.csv')
    await processor.run(batch_size=5)
if __name__ == "__main__":
    asyncio.run(main())
