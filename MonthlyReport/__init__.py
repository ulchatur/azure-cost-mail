import datetime
import json
import requests
import os
import io
import logging
import traceback
import csv
import base64
from azure.communication.email import EmailClient
import azure.functions as func

# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def get_access_token():
    """Get Azure access token using service principal credentials"""
    try:
        logger.info("Starting token acquisition...")
        
        TENANT_ID = os.environ.get("TENANT_ID")
        CLIENT_ID = os.environ.get("CLIENT_ID")
        CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
        
        # Detailed validation
        if not TENANT_ID:
            raise ValueError("TENANT_ID environment variable is not set")
        if not CLIENT_ID:
            raise ValueError("CLIENT_ID environment variable is not set")
        if not CLIENT_SECRET:
            raise ValueError("CLIENT_SECRET environment variable is not set")
        
        logger.info(f"TENANT_ID: {TENANT_ID[:8]}... (length: {len(TENANT_ID)})")
        logger.info(f"CLIENT_ID: {CLIENT_ID[:8]}... (length: {len(CLIENT_ID)})")
        logger.info(f"CLIENT_SECRET: {'*' * 8}... (length: {len(CLIENT_SECRET)})")
        
        url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token"
        payload = {
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "resource": "https://management.azure.com/"
        }
        
        logger.info(f"Requesting token from: {url}")
        response = requests.post(url, data=payload, timeout=30)
        
        if response.status_code != 200:
            logger.error(f"Token request failed with status {response.status_code}")
            logger.error(f"Response: {response.text}")
            response.raise_for_status()
        
        token_data = response.json()
        logger.info("Access token acquired successfully")
        return token_data["access_token"]
        
    except requests.exceptions.Timeout as e:
        logger.error(f"Timeout while getting access token: {str(e)}")
        raise Exception(f"Authentication timeout: {str(e)}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error getting access token: {str(e)}")
        if hasattr(e.response, 'text'):
            logger.error(f"Error response: {e.response.text}")
        raise Exception(f"Authentication failed: {str(e)}")
    except KeyError as e:
        logger.error(f"Missing key in token response: {str(e)}")
        raise Exception(f"Invalid token response: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error getting access token: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

def get_previous_month_range():
    """Calculate the first and last day of the previous month"""
    try:
        today = datetime.date.today()
        first_day_this_month = today.replace(day=1)
        last_day_prev_month = first_day_this_month - datetime.timedelta(days=1)
        first_day_prev_month = last_day_prev_month.replace(day=1)
        
        # For API calls, use ISO format (yyyy-mm-dd)
        start_date_api = first_day_prev_month.isoformat()
        end_date_api = last_day_prev_month.isoformat()
        
        # For display and filename, use mm-dd-yyyy format
        start_date_display = first_day_prev_month.strftime("%m-%d-%Y")
        end_date_display = last_day_prev_month.strftime("%m-%d-%Y")
        
        logger.info(f"Date range calculated: {start_date_display} to {end_date_display}")
        
        return start_date_api, end_date_api, start_date_display, end_date_display
        
    except Exception as e:
        logger.error(f"Error calculating date range: {str(e)}")
        raise

def get_all_subscriptions(token):
    """Fetch all subscriptions accessible to the service principal"""
    try:
        logger.info("Fetching subscriptions...")
        url = "https://management.azure.com/subscriptions?api-version=2020-01-01"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code != 200:
            logger.error(f"Subscription fetch failed with status {response.status_code}")
            logger.error(f"Response: {response.text}")
            response.raise_for_status()
        
        subscriptions = response.json().get("value", [])
        logger.info(f"Found {len(subscriptions)} subscriptions")
        
        if not subscriptions:
            logger.warning("No subscriptions found for this service principal")
        else:
            for sub in subscriptions[:3]:  # Log first 3 subscriptions
                logger.info(f"  - {sub.get('displayName')} ({sub.get('subscriptionId')})")
        
        return subscriptions
        
    except requests.exceptions.Timeout as e:
        logger.error(f"Timeout fetching subscriptions: {str(e)}")
        raise Exception(f"Subscription fetch timeout: {str(e)}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching subscriptions: {str(e)}")
        if hasattr(e.response, 'text'):
            logger.error(f"Error response: {e.response.text}")
        raise Exception(f"Failed to fetch subscriptions: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error fetching subscriptions: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

def fetch_cost_for_subscription(token, subscription_id, start_date, end_date):
    """Fetch cost data for a specific subscription"""
    try:
        logger.info(f"Fetching cost for subscription: {subscription_id}")
        
        url = f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.CostManagement/query?api-version=2023-03-01"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        body = {
            "type": "ActualCost",
            "timeframe": "Custom",
            "timePeriod": {
                "from": start_date,
                "to": end_date
            },
            "dataset": {
                "granularity": "None",
                "aggregation": {
                    "totalCost": {
                        "name": "Cost",
                        "function": "Sum"
                    }
                }
            }
        }
        
        response = requests.post(url, headers=headers, json=body, timeout=60)
        
        if response.status_code != 200:
            logger.warning(f"Cost fetch failed for {subscription_id} with status {response.status_code}")
            logger.warning(f"Response: {response.text}")
            return {"properties": {"rows": [], "columns": []}}
        
        cost_data = response.json()
        rows = cost_data.get("properties", {}).get("rows", [])
        logger.info(f"  Cost data retrieved: {len(rows)} rows")
        
        return cost_data
        
    except requests.exceptions.Timeout as e:
        logger.warning(f"Timeout fetching cost for {subscription_id}: {str(e)}")
        return {"properties": {"rows": [], "columns": []}}
    except Exception as e:
        logger.warning(f"Error fetching cost for {subscription_id}: {str(e)}")
        return {"properties": {"rows": [], "columns": []}}

def generate_csv(all_costs_data, start_date_display, end_date_display):
    """Generate CSV with all subscriptions cost data"""
    try:
        logger.info("Generating CSV file...")
        
        csv_buffer = io.StringIO()
        csv_writer = csv.writer(csv_buffer)
        
        # Write headers
        csv_writer.writerow([
            "Subscription Name",
            "Subscription ID",
            f"Total Cost ({start_date_display} to {end_date_display})",
            "Currency"
        ])
        
        total_cost = 0
        
        # Write data for each subscription
        for cost_item in all_costs_data:
            sub_name = cost_item["subscription_name"]
            sub_id = cost_item["subscription_id"]
            cost_data = cost_item["cost_data"]
            
            rows = cost_data.get("properties", {}).get("rows", [])
            
            if rows:
                # Extract cost and currency from the first row
                cost = rows[0][0] if len(rows[0]) > 0 else 0
                currency = rows[0][1] if len(rows[0]) > 1 else "USD"
                total_cost += cost
                
                csv_writer.writerow([
                    sub_name,
                    sub_id,
                    f"{cost:.2f}",
                    currency
                ])
            else:
                csv_writer.writerow([
                    sub_name,
                    sub_id,
                    "0.00",
                    "USD"
                ])
        
        # Write total row
        csv_writer.writerow([])
        csv_writer.writerow([
            "TOTAL",
            "",
            f"{total_cost:.2f}",
            "USD"
        ])
        
        csv_content = csv_buffer.getvalue()
        logger.info(f"CSV generated with {len(all_costs_data)} subscriptions, Total cost: ${total_cost:.2f}")
        
        return csv_content, total_cost
        
    except Exception as e:
        logger.error(f"Error generating CSV: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

def send_email_with_csv_attachment(csv_content, filename, start_date_display, end_date_display, total_cost, subscription_count):
    """Send email with CSV attachment using Azure Communication Services"""
    try:
        logger.info("Preparing to send email via Azure Communication Services...")
        
        # Get environment variables
        ACS_CONNECTION_STRING = os.environ.get("ACS_CONNECTION_STRING")
        ACS_SENDER_EMAIL = os.environ.get("ACS_SENDER_EMAIL")
        ACS_RECIPIENT_EMAIL = os.environ.get("ACS_RECIPIENT_EMAIL")
        
        # Validate required environment variables
        if not ACS_CONNECTION_STRING:
            raise ValueError("ACS_CONNECTION_STRING environment variable is not set")
        if not ACS_SENDER_EMAIL:
            raise ValueError("ACS_SENDER_EMAIL environment variable is not set")
        if not ACS_RECIPIENT_EMAIL:
            raise ValueError("ACS_RECIPIENT_EMAIL environment variable is not set")
        
        logger.info(f"Sender email: {ACS_SENDER_EMAIL}")
        logger.info(f"Recipient email(s): {ACS_RECIPIENT_EMAIL}")
        
        # Create EmailClient
        email_client = EmailClient.from_connection_string(ACS_CONNECTION_STRING)
        
        # Parse recipient emails (support multiple recipients separated by comma or semicolon)
        recipient_emails = [email.strip() for email in ACS_RECIPIENT_EMAIL.replace(';', ',').split(',')]
        recipient_emails = [email for email in recipient_emails if email]  # Remove empty strings
        
        if not recipient_emails:
            raise ValueError("No valid recipient emails found in ACS_RECIPIENT_EMAIL")
        
        logger.info(f"Sending to {len(recipient_emails)} recipient(s): {', '.join(recipient_emails)}")
        
        # Encode CSV to base64
        csv_base64 = base64.b64encode(csv_content.encode('utf-8')).decode('utf-8')
        
        # Create HTML email content
        html_content = f"""
        <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
            <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
                <h2 style="color: #0078d4; border-bottom: 2px solid #0078d4; padding-bottom: 10px;">
                    Azure Cost Report - Scheduled
                </h2>
                
                <p>Hello,</p>
                
                <p>This is your scheduled Azure cost report for the period:</p>
                
                <div style="background-color: #f5f5f5; padding: 15px; border-left: 4px solid #0078d4; margin: 20px 0;">
                    <strong>Report Period:</strong> {start_date_display} to {end_date_display}<br>
                    <strong>Total Subscriptions:</strong> {subscription_count}<br>
                    <strong>Total Cost:</strong> <span style="font-size: 1.2em; color: #0078d4;">${total_cost:,.2f} USD</span>
                </div>
                
                <p>The detailed cost breakdown is attached as a CSV file: <strong>{filename}</strong></p>
                
                <p style="margin-top: 30px; padding-top: 20px; border-top: 1px solid #ddd; color: #666; font-size: 0.9em;">
                    This is an automated report generated by Azure Function (Timer Trigger).<br>
                    Generated on: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")} UTC
                </p>
            </div>
        </body>
        </html>
        """
        
        # Create message structure
        message = {
            "senderAddress": ACS_SENDER_EMAIL,
            "recipients": {
                "to": [{"address": email} for email in recipient_emails]
            },
            "content": {
                "subject": f"Azure Cost Report - {start_date_display} to {end_date_display}",
                "html": html_content
            },
            "attachments": [
                {
                    "name": filename,
                    "contentType": "text/csv",
                    "contentInBase64": csv_base64
                }
            ]
        }
        
        # Send email
        logger.info("Sending email via ACS with CSV attachment...")
        poller = email_client.begin_send(message)
        result = poller.result()
        
        logger.info(f"✓ Email sent successfully!")
        logger.info(f"  Message ID: {result['id']}")
        logger.info(f"  Status: {result['status']}")
        logger.info(f"  Recipients: {', '.join(recipient_emails)}")
        logger.info(f"  Attachment: {filename}")
        
        return True
        
    except ValueError as ve:
        logger.error(f"Configuration error: {str(ve)}")
        raise
    except Exception as e:
        logger.error(f"Error sending email via ACS: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

def main(mytimer: func.TimerRequest) -> None:
    """Main function entry point for timer trigger"""
    logger.info('=' * 80)
    logger.info('Azure Cost Report - Timer Triggered Execution Starting')
    logger.info('=' * 80)
    
    # Log timer information
    if mytimer.past_due:
        logger.info('⚠️  The timer is past due!')
    
    logger.info(f'Timer trigger fired at: {datetime.datetime.utcnow()}')
    
    try:
        # Step 1: Validate environment variables
        logger.info("Step 1: Validating environment variables...")
        required_vars = [
            "TENANT_ID", 
            "CLIENT_ID", 
            "CLIENT_SECRET", 
            "ACS_CONNECTION_STRING",
            "ACS_SENDER_EMAIL",
            "ACS_RECIPIENT_EMAIL"
        ]
        missing_vars = [var for var in required_vars if not os.environ.get(var)]
        
        if missing_vars:
            error_msg = f"Missing environment variables: {', '.join(missing_vars)}"
            logger.error(error_msg)
            logger.error("Please configure these in Azure Function App Settings")
            raise ValueError(error_msg)
        
        logger.info("✓ All environment variables present")
        
        # Step 2: Get access token
        logger.info("Step 2: Acquiring Azure access token...")
        token = get_access_token()
        logger.info("✓ Access token acquired")
        
        # Step 3: Calculate date range (now returns 4 values)
        logger.info("Step 3: Calculating date range...")
        start_date_api, end_date_api, start_date_display, end_date_display = get_previous_month_range()
        logger.info(f"✓ Date range: {start_date_display} to {end_date_display}")
        
        # Step 4: Fetch subscriptions
        logger.info("Step 4: Fetching all subscriptions...")
        subscriptions = get_all_subscriptions(token)
        
        if not subscriptions:
            logger.warning("No subscriptions found")
            raise Exception("No subscriptions found - The service principal has no access to any subscriptions")
        
        logger.info(f"✓ Found {len(subscriptions)} subscriptions")
        
        # Step 5: Fetch cost data for each subscription (using API format dates)
        logger.info("Step 5: Fetching cost data for all subscriptions...")
        all_costs_data = []
        
        for idx, subscription in enumerate(subscriptions, 1):
            sub_id = subscription.get("subscriptionId")
            sub_name = subscription.get("displayName", "Unknown")
            
            logger.info(f"  [{idx}/{len(subscriptions)}] Processing: {sub_name}")
            
            cost_data = fetch_cost_for_subscription(token, sub_id, start_date_api, end_date_api)
            
            all_costs_data.append({
                "subscription_id": sub_id,
                "subscription_name": sub_name,
                "cost_data": cost_data
            })
        
        logger.info("✓ Cost data fetched for all subscriptions")
        
        # Step 6: Generate CSV file (using display format dates)
        logger.info("Step 6: Generating CSV report...")
        csv_content, total_cost = generate_csv(all_costs_data, start_date_display, end_date_display)
        filename = f"azure_cost_report_{start_date_display}_to_{end_date_display}.csv"
        logger.info("✓ CSV report generated")
        
        # Step 7: Send email with CSV attachment (using display format dates)
        logger.info("Step 7: Sending email with CSV attachment...")
        send_email_with_csv_attachment(csv_content, filename, start_date_display, end_date_display, total_cost, len(all_costs_data))
        logger.info("✓ Email sent successfully with CSV attachment")
        
        logger.info('=' * 80)
        logger.info('✅ Execution completed successfully!')
        logger.info(f'   Total Cost: ${total_cost:,.2f} USD')
        logger.info(f'   Subscriptions: {len(all_costs_data)}')
        logger.info(f'   Report: {filename}')
        logger.info('=' * 80)
        
    except ValueError as ve:
        error_msg = f"Configuration error: {str(ve)}"
        logger.error('=' * 80)
        logger.error('❌ EXECUTION FAILED - Configuration Error')
        logger.error('=' * 80)
        logger.error(error_msg)
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise
        
    except requests.exceptions.RequestException as re:
        error_msg = f"Azure API error: {str(re)}"
        logger.error('=' * 80)
        logger.error('❌ EXECUTION FAILED - API Error')
        logger.error('=' * 80)
        logger.error(error_msg)
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise
        
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error('=' * 80)
        logger.error('❌ EXECUTION FAILED - Unexpected Error')
        logger.error('=' * 80)
        logger.error(error_msg)
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise