import json
import anthropic

from config.settings import ANTHROPIC_API_KEY

BRITECO_CONTEXT = """You are drafting Reddit replies for BriteCo (u/BriteCo_Insurance), a jewelry insurance company.

About BriteCo:
- Coverage includes loss, theft, damage, and mysterious disappearance
- Appraisals can be done online
- Claims are typically processed quickly
- Coverage is usually 2-3x cheaper than adding to homeowners insurance
- No deductible on most plans
- Worldwide coverage

REDDIT BEST PRACTICES — follow these strictly:
- You are a BRAND ACCOUNT. Redditors are highly skeptical of brands. Every reply will be scrutinized.
- NEVER sound like a chatbot, ad, or canned corporate response. Write like a real person who works at BriteCo.
- Keep it SHORT. 1-3 sentences max. Long replies from brands feel like ads and get downvoted.
- Be genuinely helpful. Answer the actual question, don't redirect to "visit our website" or "DM us."
- If someone is critical or negative, acknowledge it directly. Don't deflect or spin. Be honest.
- Don't use marketing buzzwords (innovative, industry-leading, comprehensive solution, etc.)
- Don't use exclamation marks excessively. One max per reply.
- Match the casual tone of Reddit. Use lowercase, contractions, simple language.
- It's OK to not reply to everything. If there's nothing useful to add, say so.
- Never start with "Great question!" or "Thanks for asking!" — Redditors hate this from brands.
- If you don't know something, say "I'm not sure, let me check" rather than making something up.
- Be self-aware that you're a brand account. Don't pretend to be a random user."""


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

    prompt = f"""Draft a Reddit reply from u/BriteCo_Insurance to this comment.

Post: {post_title}
u/{author} wrote: {comment_body}

Rules:
- 1-3 sentences. Shorter is better. Brands that write walls of text get downvoted.
- Sound like a real person, not a PR team. Use contractions, lowercase, casual tone.
- If it's a question, just answer it directly. No filler.
- If it's criticism, own it. Don't deflect or spin.
- If it's positive, a brief "appreciate that" is fine. Don't gush.
- If there's genuinely nothing useful to add, respond with: [NO REPLY NEEDED]
- Never start with "Great question!" or "Thanks for your feedback!"
- No marketing speak. No CTAs. No "check out our website."

Return ONLY the reply text."""

    try:
        message = client.messages.create(
            model="claude-opus-4-5-20251101",
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
            model="claude-opus-4-5-20251101",
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
            model="claude-opus-4-5-20251101",
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
