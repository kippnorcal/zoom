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

    def drop_table(self, table_name):
        """Drop table before loading new data to reset schema on load"""
        try:
            table = self.sql.table(table_name)
            self.sql.engine.execute(DropTable(table))
        except NoSuchTableError:
            logging.debug(
                f"No such table {table_name} exists to drop. This command ignored."
            )

    @retry(**RETRY_PARAMS)
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

    def _get_meeting_ids(self):
        """Get list of meeting ids to iterate API calls for past meetings"""
        if self.sql.engine.has_table("Zoom_Participants", schema=self.sql.schema):
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

    @retry(**RETRY_PARAMS)
    def load_participants(self):
        """Load Zoom meeting participants"""
        table_name = "Zoom_Participants"
        for uuid in self._get_meeting_ids():
            page_token = True
            params = {"meeting_id": uuid, "page_size": 300, "type": "past"}
            while page_token:
                response = self.client.metric.list_participants(**params).json()
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

    @retry(**RETRY_PARAMS)
    def load_group_members(self):
        """Load Zoom group member data"""
        table_name = "Zoom_GroupMembers"
        self.drop_table(table_name)
        for group_id in self._get_group_ids():
            page = 1
            params = {"groupid": group_id, "page_size": 300, "page_number": page}
            response = self.client.group.list_members(**params).json()
            page_count = response["page_count"]
            while page <= page_count:
                response = self.client.group.list_members(**params).json()
                results = response.get("members")
                if results:
                    members = pd.DataFrame(results)
                    members["groupId"] = group_id
                    self.sql.insert_into(table_name, members)
                    logging.info(f"Inserted {len(members)} records into {table_name}")
                    page += 1
                    params["page"] = page

    def _get_students(self):
        """Query database for new students without Zoom accounts"""
        return pd.read_sql_table(
            "vw_Zoom_NewStudentAccounts", con=self.sql.engine, schema=self.sql.schema
        )

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

    @retry(**RETRY_PARAMS)
    def load_meetings(self):
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
            print(params)
            response = self.client.metric.list_meetings(**params).json()
            results = response.get("meetings")
            if results:
                meetings.extend(results)
            page_token = response.get("next_page_token")
            params["next_page_token"] = page_token
        if meetings:
            df = pd.DataFrame(meetings)
            self.sql.insert_into(table_name, df)
            logging.debug(f"Inserted {len(df)} meeting records into {table_name}")

    def get_school_start_date(self):
        today = datetime.datetime.today()
        if today.month > 6:
            year = today.year
        else:
            year = today.year - 1
        return datetime.date(year, 8, 1)

    def get_last_meeting_date(self):
        date = self.get_school_start_date()
        if self.sql.engine.has_table("Zoom_Meetings", schema=self.sql.schema):
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


def main():
    config.set_logging()
    connector = Connector()
    connector.load_users()
    connector.load_groups()
    connector.load_group_members()
    connector.create_student_accounts()
    connector.load_meetings()
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
