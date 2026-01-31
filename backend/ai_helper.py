import json
import anthropic

from config.settings import ANTHROPIC_API_KEY

BRITECO_CONTEXT = """You are a social media assistant for BriteCo, a jewelry insurance company.
BriteCo provides affordable, comprehensive jewelry insurance with easy online enrollment.
Key facts:
- Coverage includes loss, theft, damage, and mysterious disappearance
- Appraisals can be done online
- Claims are typically processed quickly
- Coverage is usually 2-3x cheaper than adding to homeowners insurance
- No deductible on most plans
- Worldwide coverage

Tone: Friendly, helpful, professional but not overly corporate. Empathetic to concerns.
Keep replies concise and Reddit-appropriate (casual but informative)."""


def _get_client():
    """Get Anthropic client. Returns None if API key not configured."""
    if not ANTHROPIC_API_KEY:
        return None
    return anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


def suggest_reply(comment_body, post_title, author):
    """Generate a suggested reply to a Reddit comment using Claude.

    Returns dict with 'reply' text and 'tone' description, or error info.
    """
    client = _get_client()
    if not client:
        return {'success': False, 'error': 'Anthropic API key not configured'}

    prompt = f"""A Reddit user commented on a BriteCo post. Draft a reply from BriteCo's account.

Post title: {post_title}
Comment by u/{author}: {comment_body}

Write a helpful, friendly reply (2-4 sentences max). If it's negative feedback, be empathetic and offer to help. If it's a question, answer it. If it's positive, thank them briefly. Don't be overly salesy. Don't use emojis excessively.

Return ONLY the reply text, nothing else."""

    try:
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=300,
            system=BRITECO_CONTEXT,
            messages=[{"role": "user", "content": prompt}]
        )
        reply_text = message.content[0].text.strip()
        return {'success': True, 'reply': reply_text}
    except Exception as e:
        return {'success': False, 'error': str(e)}


def analyze_sentiment(comment_body):
    """Analyze the sentiment of a comment using Claude.

    Returns one of: 'positive', 'negative', 'question', 'neutral'
    """
    client = _get_client()
    if not client:
        return 'neutral'

    prompt = f"""Classify this Reddit comment's sentiment into exactly one category.
Return ONLY one word: positive, negative, question, or neutral.

Comment: {comment_body}"""

    try:
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=10,
            messages=[{"role": "user", "content": prompt}]
        )
        result = message.content[0].text.strip().lower()
        if result in ('positive', 'negative', 'question', 'neutral'):
            return result
        return 'neutral'
    except Exception:
        return 'neutral'


def batch_analyze_sentiment(comments):
    """Analyze sentiment for multiple comments in a single API call.

    Takes a list of comment dicts (must have 'id' and 'body' keys).
    Returns dict mapping comment_id -> sentiment.
    """
    client = _get_client()
    if not client:
        return {c['id']: 'neutral' for c in comments}

    if not comments:
        return {}

    # Batch up to 20 comments per call to keep token usage reasonable
    batch = comments[:20]
    comment_list = "\n".join(
        f"[{c['id']}]: {c['body'][:200]}"
        for c in batch
    )

    prompt = f"""Classify each Reddit comment's sentiment. For each comment ID, return exactly one label: positive, negative, question, or neutral.

Comments:
{comment_list}

Return ONLY a JSON object mapping comment IDs to sentiments, like:
{{"id1": "positive", "id2": "question"}}"""

    try:
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )
        result_text = message.content[0].text.strip()
        # Extract JSON from response
        if '{' in result_text:
            json_str = result_text[result_text.index('{'):result_text.rindex('}') + 1]
            result = json.loads(json_str)
            # Validate values
            valid = ('positive', 'negative', 'question', 'neutral')
            return {k: (v if v in valid else 'neutral') for k, v in result.items()}
    except Exception:
        pass

    return {c['id']: 'neutral' for c in batch}
