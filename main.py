import datetime
import logging
import os
import time
import traceback
from urllib.parse import quote

import pandas as pd
import requests
from sqlalchemy.schema import DropTable
from sqlalchemy import inspect
from sqlalchemy.exc import NoSuchTableError
from tenacity import retry, wait_exponential, stop_after_attempt
from zoomus import ZoomClient

import config
from mailer import Mailer
from timer import elapsed


RETRY_PARAMS = {
    "stop": stop_after_attempt(3),
    "wait": wait_exponential(multiplier=2, min=4, max=10),
}


class Connector:
    """
    Data connector for extracting Zoom data from API, transforming into
    dataframes, and loading into database.
    """

    def __init__(self):
        self.client = ZoomClient(config.ZOOM_KEY, config.ZOOM_SECRET)
        self.sql = config.db_connection()
        self.inspector = inspect(self.sql.engine)

    def drop_table(self, table_name):
        """Drop table before loading new data to reset schema on load"""
        try:
            table = self.sql.table(table_name)
            self.sql.engine.execute(DropTable(table))
        except NoSuchTableError:
            logging.debug(
                f"No such table {table_name} exists to drop. This command ignored."
            )

    @elapsed
    @retry(**RETRY_PARAMS)
    def load_users(self):
        """Load Zoom user data in order to query meetings"""
        page = 1
        table_name = "Zoom_Users"
        total_users = 0
        response = self.client.user.list(page_size=300, page_number=page).json()
        page_count = response["page_count"]
        if response:
            self.drop_table(table_name)
        while page <= page_count:
            response = self.client.user.list(page_size=300, page_number=page).json()
            results = response.get("users")
            if results:
                total_users += len(results)
                users = pd.DataFrame(results)
                users = users.reindex(columns=config.USER_COLUMNS)
                self.sql.insert_into(table_name, users)
                logging.debug(f"Inserted {len(users)} records into {table_name}")
                page += 1
        logging.info(f"Inserted {total_users} users into {table_name}")

    def _get_meeting_uuids(self):
        """
        Get list of all meeting uuids that there is not participants
        data for yet in the database
        """
        if self.inspector.has_table(
            table_name="Zoom_Participants", schema=self.sql.schema
        ):
            meetings = self.sql.query(
                """SELECT DISTINCT zm.uuid
                FROM custom.Zoom_Meetings zm
                LEFT JOIN custom.Zoom_Participants zp
                    ON zm.uuid = zp.meeting_uuid
                WHERE zp.meeting_uuid IS NULL"""
            )
        else:
            meetings = pd.read_sql_table(
                table_name="Zoom_Meetings", con=self.sql.engine, schema=self.sql.schema
            )
        meeting_ids = meetings["uuid"].values.flatten().tolist()
        return meeting_ids

    @elapsed
    @retry(**RETRY_PARAMS)
    def load_participants(self):
        """Load Zoom meeting participants"""
        table_name = "Zoom_Participants"
        uuids = self._get_meeting_uuids()
        total_participants = 0
        for uuid in uuids:
            page_token = True
            params = {"meeting_id": uuid, "page_size": 300, "type": "past"}
            while page_token:
                response = self.client.metric.list_participants(**params).json()
                if response.get("code") == 429:
                    logging.info("Rate limit reached; waiting 10 seconds...")
                    time.sleep(10)
                results = response.get("participants")
                if results:
                    results = [dict(item, meeting_uuid=uuid) for item in results]
                    total_participants += len(results)
                    participants = pd.DataFrame(results)
                    self.sql.insert_into(table_name, participants)
                    logging.debug(
                        f"Inserted  {len(participants)} participants for meeting {uuid} into {table_name}"
                    )
                page_token = response.get("next_page_token")
                params["next_page_token"] = page_token
        logging.info(
            f"Inserted {total_participants} participants for {len(uuids)} meetings into {table_name}"
        )

    @elapsed
    @retry(**RETRY_PARAMS)
    def load_groups(self):
        """Load Zoom permission groups"""
        table_name = "Zoom_Groups"
        response = self.client.group.list().json()
        if response:
            self.drop_table(table_name)
        results = response.get("groups")
        if results:
            groups = pd.DataFrame(results)
            self.sql.insert_into(table_name, groups)
            logging.info(f"Inserted {len(groups)} records into {table_name}")

    def _get_group_ids(self, groupname=None):
        """Get list of group ids to iterate API calls for group members"""
        groups = pd.read_sql_table(
            table_name="Zoom_Groups", con=self.sql.engine, schema=self.sql.schema
        )
        if groupname:
            groups = groups[groups["name"] == groupname]
        group_ids = groups["id"].values.flatten().tolist()
        return group_ids

    @elapsed
    @retry(**RETRY_PARAMS)
    def load_group_members(self):
        """Load Zoom group member data"""
        table_name = "Zoom_GroupMembers"
        self.drop_table(table_name)
        total_group_members = 0
        for group_id in self._get_group_ids():
            page = 1
            params = {"groupid": group_id, "page_size": 300, "page_number": page}
            response = self.client.group.list_members(**params).json()
            page_count = response["page_count"]
            while page <= page_count:
                response = self.client.group.list_members(**params).json()
                results = response.get("members")
                if results:
                    total_group_members += len(results)
                    members = pd.DataFrame(results)
                    members["groupId"] = group_id
                    self.sql.insert_into(table_name, members)
                    logging.debug(f"Inserted {len(members)} records into {table_name}")
                    page += 1
                    params["page"] = page
        logging.info(f"Inserted {total_group_members} group members into {table_name}")

    def _get_students(self):
        """Query database for new students without Zoom accounts"""
        return pd.read_sql_table(
            "vw_Zoom_NewStudentAccounts", con=self.sql.engine, schema=self.sql.schema
        )

    @elapsed
    def create_student_accounts(self):
        """Create Zoom accounts for students and add to Students group"""
        students = self._get_students()
        BASIC_USER = 1
        students["type"] = BASIC_USER
        students = students.to_dict(orient="records")
        emails = [{"email": student["email"]} for student in students]
        student_group_id = self._get_group_ids("Students")[0]
        # Create accounts
        for student in students:
            try:
                r = self.client.user.create(action="create", user_info=student)
                r.raise_for_status()
                logging.info(f"Created account for {student['email']}")
            except requests.exceptions.HTTPError as e:
                logging.error(e)
        # Add to Students group
        try:
            r = self.client.group.add_members(groupid=student_group_id, members=emails)
            r.raise_for_status()
            logging.info(f"Added {len(emails)} new users to Students group.")
        except requests.exceptions.HTTPError as e:
            logging.error(e)
            logging.error(r.text)

    @elapsed
    @retry(**RETRY_PARAMS)
    def load_meetings(self):
        """
        Load Zoom Meetings data for the prior day based on the last
        date data is available for in the database. If no prior data
        exists, it will pull from the start of August for the current
        school year.
        """
        run_date = self.get_last_meeting_date()
        if run_date >= datetime.datetime.today().date():
            return
        table_name = "Zoom_Meetings"
        page_token = True
        meetings = []
        params = {
            "page_size": 300,
            "type": "past",
            "from": run_date,
            "to": run_date,
        }
        while page_token:
            response = self.client.metric.list_meetings(**params).json()
            if response.get("code") == 429:
                logging.info("Rate limit reached; waiting 60 seconds...")
                time.sleep(60)
            results = response.get("meetings")
            if results:
                meetings.extend(results)
            page_token = response.get("next_page_token")
            params["next_page_token"] = page_token
        if meetings:
            df = pd.DataFrame(meetings)
            self.sql.insert_into(table_name, df)
            logging.info(
                f"Inserted {len(df)} meeting records into {table_name} for {run_date.strftime('%Y-%m-%d')}"
            )

    def get_school_start_date(self):
        """Get the first day in Aug of the current school year"""
        today = datetime.datetime.today()
        if today.month > 6:
            year = today.year
        else:
            year = today.year - 1
        return datetime.date(year, 8, 1)

    def get_last_meeting_date(self):
        """
        Get the last date that meeting data is available for in the
        database, otherwise return the first day in August.
        """
        date = self.get_school_start_date()
        if self.inspector.has_table(table_name="Zoom_Meetings", schema=self.sql.schema):
            df = pd.read_sql_table(
                table_name="Zoom_Meetings", con=self.sql.engine, schema=self.sql.schema
            )
            previous_date = df.start_time.max()
            if len(df) > 0 and previous_date:
                previous_date = datetime.datetime.strptime(
                    previous_date, "%Y-%m-%dT%H:%M:%S%z"
                )
                date = previous_date.date() + datetime.timedelta(days=1)
        return date

    def get_meeting_settings(self):
        """Load Zoom meeting settings"""
        table_name = "Zoom_Meeting_Settings"
        meeting_ids = self._get_meeting_ids()
        total_settings = 0
        for meeting_id in meeting_ids:
            page_token = True
            params = {"id": meeting_id, "page_size": 300, "type": "past"}
            results = []
            while page_token:
                response = self.client.meeting.get(**params).json()
                # HTTP Error 429: Too Many Requests
                if response.get("code") == 429:
                    logging.info("Rate limit reached; waiting 10 seconds...")
                    time.sleep(10)
                # Zoom Error 3001: Meeting does not exist
                if response.get("code") == 3001:
                    logging.debug(response.get("message"))
                settings = response.get("settings", {})
                results.append(self._format_settings_data(settings, meeting_id))
                page_token = response.get("next_page_token")
                params["next_page_token"] = page_token

            if results:
                total_settings += len(results)
                settings = pd.DataFrame(results)
                self.sql.insert_into(table_name, settings)
                logging.debug(
                    f"Inserted  {len(settings)} settings for meeting {meeting_id} into {table_name}"
                )

        logging.info(
            f"Inserted {total_settings} settings for {len(meeting_ids)} meetings into {table_name}"
        )

    def _get_meeting_ids(self):
        """
        Get list of all meeting ids that there is not settings 
        data for yet in the database
        """
        if self.sql.engine.has_table("Zoom_Meeting_Settings", schema=self.sql.schema):
            meetings = self.sql.query(
                """SELECT DISTINCT zm.id
                FROM custom.Zoom_Meetings zm
                LEFT JOIN custom.Zoom_Meeting_Settings zms
                    ON zm.id = zms.meeting_id
                WHERE zms.meeting_id IS NULL"""
            )
        else:
            meetings = pd.read_sql_table(
                table_name="Zoom_Meetings", con=self.sql.engine, schema=self.sql.schema
            )
        meeting_ids = meetings["id"].values.flatten().tolist()
        return meeting_ids

    def _format_settings_data(self, item, meeting_id):
        """Extract meeting settings fields related to authentication."""
        return {
            "meeting_id": meeting_id,
            "enforce_login": item.get("enforce_login"),
            "enforce_login_domains": item.get("enforce_login_domains"),
            "waiting_room": item.get("waiting_room"),
            "meeting_authentication": item.get("meeting_authentication"),
            "authentication_domains": item.get("authentication_domains"),
            "authentication_name": item.get("authentication_name"),
        }


def main():
    config.set_logging()
    connector = Connector()
    if config.USERS:
        connector.load_users()
        connector.load_groups()
        connector.load_group_members()
    if config.ACCOUNTS:
        connector.create_student_accounts()
    if config.MEETINGS:
        connector.load_meetings()
        connector.load_participants()
        connector.get_meeting_settings()


if __name__ == "__main__":
    try:
        main()
        error_message = None
    except Exception as e:
        logging.exception(e)
        error_message = traceback.format_exc()
    if config.ENABLE_MAILER:
        Mailer("Zoom Connector").notify(error_message=error_message)
