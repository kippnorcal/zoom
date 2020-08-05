import datetime
import logging
import os
import traceback
from urllib.parse import quote

import pandas as pd
from sqlalchemy.schema import DropTable
from sqlalchemy.exc import NoSuchTableError
from tenacity import retry, wait_exponential, stop_after_attempt
from zoomus import ZoomClient

import config
from mailer import Mailer


class Connector:
    """
    Data connector for extracting Zoom data from API, transforming into
    dataframes, and loading into database.
    """

    def __init__(self):
        self.client = ZoomClient(config.ZOOM_KEY, config.ZOOM_SECRET)
        self.sql = config.db_connection()

    def drop_table(self, table_name):
        """Drop table before loading new data to reset schema on load"""
        try:
            table = self.sql.table(table_name)
            self.sql.engine.execute(DropTable(table))
        except NoSuchTableError:
            logging.debug(
                f"No such table {table_name} exists to drop. This command ignored."
            )

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=10)
    )
    def load_users(self):
        """Load Zoom user data in order to query meetings"""
        page = 1
        table_name = "Zoom_Users"
        response = self.client.user.list(page_size=300, page_number=page).json()
        page_count = response["page_count"]
        if response:
            self.drop_table(table_name)
        while page <= page_count:
            response = self.client.user.list(page_size=300, page_number=page).json()
            results = response.get("users")
            if results:
                users = pd.DataFrame(results)
                users = users.reindex(columns=config.USER_COLUMNS)
                self.sql.insert_into(table_name, users)
                logging.info(f"Inserted {len(users)} records into {table_name}")
                page += 1

    def _get_user_ids(self):
        """Get list of user ids to iterate API calls for meetings"""
        users = pd.read_sql_table(
            table_name="Zoom_Users", con=self.sql.engine, schema=self.sql.schema
        )
        user_ids = users["id"].values.flatten().tolist()
        return user_ids

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=10)
    )
    def load_meetings(self):
        """Load Zoom meeting data in order to query past meetings"""
        table_name = "Zoom_Meetings"
        self.drop_table(table_name)
        for user_id in self._get_user_ids():
            page_token = True
            meetings = []
            while page_token:
                response = self.client.meeting.list(
                    user_id=user_id, page_size=300, page_token=page_token
                ).json()
                results = response.get("meetings")
                meetings.extend(results)
                page_token = response["next_page_token"]
            if meetings:
                meetings = pd.DataFrame(results)
                meetings = meetings.reindex(columns=config.MEETING_COLUMNS)
                self.sql.insert_into(table_name, meetings)
                logging.debug(
                    f"Inserted {len(meetings)} meeting records for user {user_id} into {table_name}"
                )

    def _get_meeting_ids(self):
        """Get list of meeting ids to iterate API calls for past meetings"""
        meetings = pd.read_sql_table(
            table_name="Zoom_Meetings", con=self.sql.engine, schema=self.sql.schema
        )
        meetings = meetings[
            meetings["start_time"] < datetime.datetime.now().strftime("%Y-%m-%d")
        ]
        meeting_ids = meetings["id"].values.flatten().tolist()
        return meeting_ids

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=10)
    )
    def load_past_meetings(self):
        """Load Zoom past meeting data in order to query participants"""
        table_name = "Zoom_PastMeetings"
        self.drop_table(table_name)
        for meeting_id in self._get_meeting_ids():
            page_token = True
            params = {"meeting_id": str(meeting_id), "page_size": 300}
            while page_token:
                response = self.client.past_meeting.list(**params).json()
                results = response.get("meetings")
                if results:
                    results = [dict(item, meeting_id=meeting_id) for item in results]
                    past_meetings = pd.DataFrame(results)
                    self.sql.insert_into(table_name, past_meetings)
                    logging.info(
                        f"Inserted {len(past_meetings)} past meeting records for meeting {meeting_id} into {table_name}"
                    )
                page_token = response.get("next_page_token")
                params["next_page_token"] = page_token

    def _get_past_meeting_uuids(self):
        """Get list of past meeting uuids to iterate API calls for participants"""
        meetings = pd.read_sql_table(
            table_name="Zoom_PastMeetings", con=self.sql.engine, schema=self.sql.schema
        )
        meeting_uuids = meetings["uuid"].values.flatten().tolist()
        return meeting_uuids

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=10)
    )
    def load_participants(self):
        """Load Zoom meeting participants"""
        table_name = "Zoom_Participants"
        self.drop_table(table_name)
        for uuid in self._get_past_meeting_uuids():
            page_token = True
            params = {"meeting_id": uuid, "page_size": 300}
            while page_token:
                response = self.client.past_meeting.get_participants(**params).json()
                results = response.get("participants")
                if results:
                    results = [dict(item, meeting_uuid=uuid) for item in results]
                    participants = pd.DataFrame(results)
                    self.sql.insert_into(table_name, participants)
                    logging.info(
                        f"Inserted  {len(participants)} participants for meeting {uuid} into {table_name}"
                    )
                page_token = response.get("next_page_token")
                params["next_page_token"] = page_token


def main():
    config.set_logging()
    connector = Connector()
    connector.load_users()
    connector.load_meetings()
    connector.load_past_meetings()
    connector.load_participants()


if __name__ == "__main__":
    try:
        main()
        error_message = None
    except Exception as e:
        logging.exception(e)
        error_message = traceback.format_exc()
    if config.ENABLE_MAILER:
        Mailer("Zoom Connector").notify(error_message=error_message)
