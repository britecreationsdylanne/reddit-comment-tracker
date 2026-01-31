from datetime import datetime, timezone
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Content, HtmlContent

from config.settings import SENDGRID_API_KEY, SENDGRID_FROM_EMAIL, SENDGRID_FROM_NAME
from backend.database import get_recipients


def _build_email_html(new_comments, post_summary):
    """Build the HTML email body showing new comments grouped by post."""

    comment_rows = ""
    for comment in new_comments[:50]:  # cap at 50 to keep email reasonable
        created = datetime.fromtimestamp(comment['created_utc'], tz=timezone.utc)
        date_str = created.strftime('%b %d, %Y at %I:%M %p UTC')
        body_preview = comment['body'][:200] + ('...' if len(comment['body']) > 200 else '')
        post_title_short = comment['post_title'][:60] + ('...' if len(comment['post_title']) > 60 else '')

        comment_rows += f"""
        <tr>
            <td style="padding: 12px; border-bottom: 1px solid #e0e0e0;">
                <div style="font-weight: 600; color: #272d3f;">u/{comment['author']}</div>
                <div style="color: #666; font-size: 12px; margin-top: 2px;">on <a href="{comment['post_url']}" style="color: #008181;">{post_title_short}</a></div>
                <div style="color: #666; font-size: 12px;">r/{comment['subreddit']} &middot; {date_str}</div>
            </td>
            <td style="padding: 12px; border-bottom: 1px solid #e0e0e0; color: #333;">
                {body_preview}
            </td>
        </tr>
        """

    post_summary_rows = ""
    for post in post_summary:
        post_summary_rows += f"""
        <tr>
            <td style="padding: 8px 12px; border-bottom: 1px solid #e0e0e0;">
                <a href="{post['url']}" style="color: #008181; text-decoration: none;">{post['title'][:70]}</a>
            </td>
            <td style="padding: 8px 12px; border-bottom: 1px solid #e0e0e0; text-align: center; font-weight: 600;">
                {post['new_count']}
            </td>
            <td style="padding: 8px 12px; border-bottom: 1px solid #e0e0e0; text-align: center; color: #666;">
                r/{post['subreddit']}
            </td>
        </tr>
        """

    total_new = len(new_comments)
    total_posts = len(post_summary)
    now_str = datetime.now(timezone.utc).strftime('%B %d, %Y at %I:%M %p UTC')

    html = f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="utf-8"></head>
    <body style="margin: 0; padding: 0; font-family: Arial, Helvetica, sans-serif; background-color: #f5f5f5;">
        <table width="100%" cellpadding="0" cellspacing="0" style="max-width: 700px; margin: 0 auto; background-color: #ffffff;">
            <!-- Header -->
            <tr>
                <td colspan="3" style="background-color: #272d3f; padding: 24px 30px;">
                    <h1 style="color: #ffffff; margin: 0; font-size: 22px;">BriteCo Reddit Comment Tracker</h1>
                    <p style="color: #31D7CA; margin: 5px 0 0 0; font-size: 14px;">New Activity Report</p>
                </td>
            </tr>

            <!-- Summary -->
            <tr>
                <td colspan="3" style="padding: 24px 30px; background-color: #f8fffe; border-bottom: 2px solid #31D7CA;">
                    <h2 style="margin: 0 0 5px 0; color: #272d3f; font-size: 18px;">
                        {total_new} new comment{'s' if total_new != 1 else ''} across {total_posts} post{'s' if total_posts != 1 else ''}
                    </h2>
                    <p style="margin: 0; color: #666; font-size: 13px;">Scanned on {now_str}</p>
                </td>
            </tr>

            <!-- Post Summary -->
            <tr>
                <td colspan="3" style="padding: 20px 30px 10px 30px;">
                    <h3 style="color: #272d3f; margin: 0 0 10px 0; font-size: 15px;">Posts with New Comments</h3>
                    <table width="100%" cellpadding="0" cellspacing="0" style="font-size: 14px;">
                        <tr style="background-color: #f0f0f0;">
                            <td style="padding: 8px 12px; font-weight: 600;">Post</td>
                            <td style="padding: 8px 12px; font-weight: 600; text-align: center;">New</td>
                            <td style="padding: 8px 12px; font-weight: 600; text-align: center;">Subreddit</td>
                        </tr>
                        {post_summary_rows}
                    </table>
                </td>
            </tr>

            <!-- Recent Comments -->
            <tr>
                <td colspan="3" style="padding: 20px 30px 10px 30px;">
                    <h3 style="color: #272d3f; margin: 0 0 10px 0; font-size: 15px;">Recent Comments</h3>
                    <table width="100%" cellpadding="0" cellspacing="0" style="font-size: 14px;">
                        <tr style="background-color: #f0f0f0;">
                            <td style="padding: 8px 12px; font-weight: 600; width: 35%;">Author / Post</td>
                            <td style="padding: 8px 12px; font-weight: 600;">Comment</td>
                        </tr>
                        {comment_rows}
                    </table>
                </td>
            </tr>

            <!-- Footer -->
            <tr>
                <td colspan="3" style="padding: 20px 30px; background-color: #f5f5f5; text-align: center;">
                    <p style="color: #999; font-size: 12px; margin: 0;">
                        BriteCo Reddit Comment Tracker &middot; Automated notification
                    </p>
                </td>
            </tr>
        </table>
    </body>
    </html>
    """
    return html


def send_notification(new_comments):
    """Send email notification to all active recipients. Returns count of emails sent."""
    if not new_comments:
        return 0

    if not SENDGRID_API_KEY:
        print("[Email] SendGrid API key not configured — skipping email notification")
        return 0

    recipients = get_recipients(active_only=True)
    if not recipients:
        print("[Email] No active recipients — skipping email notification")
        return 0

    # Build post summary (group new comments by post)
    post_map = {}
    for c in new_comments:
        pid = c['post_id']
        if pid not in post_map:
            post_map[pid] = {
                'title': c['post_title'],
                'url': c['post_url'],
                'subreddit': c.get('subreddit', ''),
                'new_count': 0
            }
        post_map[pid]['new_count'] += 1

    post_summary = sorted(post_map.values(), key=lambda x: x['new_count'], reverse=True)

    total_new = len(new_comments)
    subject = f"Reddit Tracker: {total_new} new comment{'s' if total_new != 1 else ''} on BriteCo posts"
    html_body = _build_email_html(new_comments, post_summary)

    sg = SendGridAPIClient(SENDGRID_API_KEY)
    sent_count = 0

    for recipient in recipients:
        try:
            message = Mail(
                from_email=Email(SENDGRID_FROM_EMAIL, SENDGRID_FROM_NAME),
                to_emails=To(recipient['email'], recipient.get('name', '')),
                subject=subject,
                html_content=HtmlContent(html_body)
            )
            sg.send(message)
            sent_count += 1
        except Exception as e:
            print(f"[Email] Failed to send to {recipient['email']}: {e}")

    print(f"[Email] Sent {sent_count}/{len(recipients)} notification emails")
    return sent_count


def send_test_email(to_email):
    """Send a test notification email to a specific address."""
    if not SENDGRID_API_KEY:
        return False, "SendGrid API key not configured"

    test_comments = [
        {
            'author': 'test_user',
            'body': 'This is a test comment to verify email notifications are working correctly.',
            'created_utc': datetime.now(timezone.utc).timestamp(),
            'post_title': 'Test Post - Jewelry Insurance Guide',
            'post_url': 'https://www.reddit.com/r/jewelry/comments/test/',
            'post_id': 't3_test',
            'subreddit': 'jewelry'
        }
    ]
    post_summary = [{'title': 'Test Post - Jewelry Insurance Guide', 'url': 'https://www.reddit.com/r/jewelry/comments/test/', 'subreddit': 'jewelry', 'new_count': 1}]

    html_body = _build_email_html(test_comments, post_summary)

    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        message = Mail(
            from_email=Email(SENDGRID_FROM_EMAIL, SENDGRID_FROM_NAME),
            to_emails=To(to_email),
            subject="[TEST] BriteCo Reddit Tracker - Test Notification",
            html_content=HtmlContent(html_body)
        )
        sg.send(message)
        return True, "Test email sent"
    except Exception as e:
        return False, str(e)
