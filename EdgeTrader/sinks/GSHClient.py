import gspread
import datetime
from datetime import date
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build  # Import 'build' function for API requests

class GSHClient:
    def __init__(self, credentials_file, spreadsheet_key, sheet_id=None):
        # Set up Google Sheets API access
        self.scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        self.creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, self.scope)
        self.gc = gspread.authorize(self.creds)
        
        # The spreadsheet key should be provided to access the specific sheet
        self.spreadsheet_key = spreadsheet_key
        self.spreadsheet = self.gc.open_by_key(self.spreadsheet_key)
        
        # Get worksheet by ID if provided, otherwise use the first sheet
        if sheet_id:
            try:
                # Convert sheet_id to integer if it's a string
                sheet_id_int = int(sheet_id)
                self.worksheet = self.spreadsheet.get_worksheet_by_id(sheet_id_int)
                if self.worksheet is None:
                    print(f"Warning: Sheet with ID {sheet_id} not found. Using first sheet instead.")
                    self.worksheet = self.spreadsheet.sheet1
                else:
                    print(f"Successfully connected to sheet: {self.worksheet.title} (ID: {sheet_id})")
            except ValueError:
                print(f"Warning: Invalid sheet_id '{sheet_id}'. Using first sheet instead.")
                self.worksheet = self.spreadsheet.sheet1
            except Exception as e:
                print(f"Warning: Error accessing sheet with ID {sheet_id}: {e}. Using first sheet instead.")
                self.worksheet = self.spreadsheet.sheet1
        else:
            self.worksheet = self.spreadsheet.sheet1  # Use the first sheet by default
            print(f"No sheet ID provided. Using first sheet: {self.worksheet.title}")

    def write_trade_to_sheets(self, trade_data, test_mode=False, comment=None):
            if trade_data:
                try:
                    # Get the current date in the format month/day/year (e.g., 3/30/2025)
                    now = datetime.datetime.now()
                    current_date = f"{now.month}/{now.day}/{now.year}"

                    # Convert 'Duration' to a Google Sheets-compatible time format
                    if isinstance(trade_data['Duration'], datetime.timedelta):
                        # Convert timedelta to total seconds
                        total_seconds = trade_data['Duration'].total_seconds()
                        # Convert total seconds into Google Sheets time serial number (fraction of a day)
                        duration_as_time = total_seconds / 86400  # There are 86400 seconds in a day
                    else:
                        # If Duration is already in the correct format (e.g., HH:MM:SS), we can pass it as is
                        duration_as_time = trade_data['Duration']  # Google Sheets should handle this if it's valid

                    # Set comment: explicit override wins, else based on test mode
                    if comment is None:
                        if test_mode:
                            comment = "Test Trade!!! Please, remove this line!!!"
                        else:
                            comment = "Live!!!"

                    # Prepare the row data
                    row = [
                        current_date, #Date
                        None,         #Balance
                        None,         #BNB Balance
                        trade_data['win_lose'], #Win/Lose
                        trade_data['profit_trade'], #Profit
                        trade_data['total_fee_bnb'], #Total Fee BNB
                        trade_data['fee_usdt_equiv'], #Fee USDT Equiv
                        None, #Total Fee (USDT equiv.)
                        None, #ROI
                        None, #WinRate
                        None, #Av. RR
                        None, #Profit (month)
                        None, #Profit (year)
                        None, #Profit (total)
                        None, #Av. gain
                        None, #Av. loss
                        None, #Cum. ROI (month)
                        None, #Cum. ROI (year)
                        trade_data['virtual'], #Virtual
                        trade_data['good_trade'], #Good Trade
                        None, #Av. Good Trades
                        trade_data['type'], #Type
                        trade_data['pair'], #Pair
                        None, # Trades per month
                        duration_as_time, # Duration
                        None, # Av. Duration
                        comment # Comments - Test or Live
                    ]
                    
                    # Debugging: Print the row to be appended
                    print("Preparing to append the following row to the spreadsheet:")
                    print(row)

                    # Get all existing rows, which will include the header row
                    existing_rows = self.worksheet.get_all_values()

                    # The first available row to append data (skipping the header row)
                    next_row_index = len(existing_rows) + 1  # +1 because rows are 1-indexed in Sheets

                    # Insert the row below the header
                    self.worksheet.insert_row(row, next_row_index, value_input_option='USER_ENTERED')  # Insert at the next row after the header

                    print("Row successfully inserted into the spreadsheet.")

                    # Call the format function to center-align the row
                    # self.format_row(next_row_index, len(row))  # Formatting the inserted row

                    # Verify if the data was added by printing the number of rows
                    print(f"Number of rows in the sheet: {len(self.worksheet.get_all_values())}")

                except gspread.exceptions.APIError as e:
                    print(f"APIError occurred: {e}")
                except Exception as e:
                    print(f"Unexpected error occurred: {e}")

    def format_row(self, row_index, num_columns):
        """
        Format the row with center alignment and other desired formats.
        :param row_index: The row index where the data was inserted
        :param num_columns: The number of columns in the row to format
        """
        try:
            # Get the sheet ID
            sheet_id = self.worksheet.id  # Get the sheet ID

            service = build('sheets', 'v4')

            requests = [
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": row_index - 1,  # Row is 1-indexed, API uses 0-indexed
                            "endRowIndex": row_index,
                            "startColumnIndex": 0,
                            "endColumnIndex": num_columns  # Adjust column count
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "horizontalAlignment": "CENTER",  # Set horizontal alignment to center
                                "textFormat": {
                                    "bold": False  # Set bold to False, modify as needed
                                }
                            }
                        },
                        "fields": "userEnteredFormat.horizontalAlignment,userEnteredFormat.textFormat.bold"
                    }
                }
            ]

            # Execute the formatting request
            response = service.spreadsheets().batchUpdate(
                spreadsheetId=self.worksheet.spreadsheet.id,
                body={"requests": requests}
            ).execute()

            print(f"Row {row_index} formatted with center alignment successfully.")

        except Exception as e:
            print(f"Error formatting row: {e}")

    def print_trade_data(self, trade_data):
        print(f"Balance: {trade_data['balance']}")
        print(f"Win/Lose: {trade_data['win_lose']}")
        print(f"Target_Reached: {trade_data['target_reached']}")
        print(f"Profit Trade: {trade_data['profit_trade']}")
        print(f"ROI Trade: {trade_data['roi_trade']}")
        print(f"Virtual: {trade_data['virtual']}")
        print(f"Good Trade: {trade_data['good_trade']}")
        print(f"Trade Type: {trade_data['type']}")
        print(f"Pair: {trade_data['pair']}")
        print(f"Duration: {trade_data['Duration']}")


# Testing the GSHClient
if __name__ == '__main__':
    # Use the specific spreadsheet key of your existing Google Sheet
    new_spreadsheet_key = '1DhS1roLO-1CceT1qi3GSKGlCu-eWIAWakT7rbbtjV9s'
    main_spreadsheet_key = '1pAtrfi5E5qqAlsLzW0UWl1gPHydFxGEpHAo371ctDb8'

    # Example of creating a GSHClient for a specific bot
    # Test with new sheet ID
    gsh_client = GSHClient(
        credentials_file='steadfast-canto-451511-q5-285cde7ad6c5.json', 
        spreadsheet_key=main_spreadsheet_key,
        sheet_id='724594662'  # Your new sheet ID
    )

    # Example of trade data (from Binance or elsewhere)
    trade_data = {
        'balance': 1014,
        'win_lose': 1,
        'target_reached' : 1,
        'profit_trade' : -3.33,
        'roi_trade' : '-0.77%',
        'virtual': 1,
        'good_trade': 1,
        'type': 'Long',
        'pair': 'BTCUSDT',
        'Duration': '01:45:22'
    }

    # Print Trade Data
    gsh_client.print_trade_data(trade_data)

    # Write the trade data to Google Sheets
    gsh_client.write_trade_to_sheets(trade_data)