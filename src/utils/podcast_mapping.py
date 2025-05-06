from src.models.lead import PodcastLead


def map_listennotes_result_to_lead(result):
    # Dummy mapping: returns a PodcastLead using values from the result dict
    return PodcastLead(podcast_id=result.get('id', 'unknown'), name=result.get('title_original', 'No Name'))


def map_podscan_result_to_lead(result):
    # Dummy mapping: returns a PodcastLead using values from the result dict
    return PodcastLead(podcast_id=result.get('id', 'unknown'), name=result.get('title', 'No Name')) 