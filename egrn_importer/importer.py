import logging
import traceback

import psycopg2
import requests
from time import sleep

import sys

from egrn_importer.message import EgrnMessage
from egrn_importer.response import EgrnResponse
from egrn_importer.settings import STORAGE, DB_NAME, DB_USER, DB_HOST, DB_PASSWORD, DB_PORT


class EgrnImporter:
    def __init__(self) -> None:
        self.log = logging.getLogger()

    def handle_message(self, body: bytes) -> None:
        """
        Коллбэк для RabbitMQ. Парсит выписку, складывает получившееся в базу.
        """
        self.log.info('Received %r' % body)
        message = EgrnMessage.from_str(body.decode())
        self.log.debug(message)

        egrn_response = self._get_egrn_response(message.xml_id)
        self.log.debug(egrn_response)

        self._save_egrn_response(message, egrn_response)

    def _get_egrn_response(self, xml_id: int) -> EgrnResponse:
        xml_string = self._download_xml_by_id(xml_id)
        return EgrnResponse.from_str(xml_string.decode())

    @staticmethod
    def _download_xml_by_id(xml_id: int) -> bytes:
        response = requests.get('{}{}'.format(STORAGE, xml_id))
        response.raise_for_status()

        content_type = response.headers.get('Content-Type', '')
        if 'text' in content_type or 'json' in content_type:
            raise Exception('Unable to download XML for xml_id %s: probably 404'.format(xml_id))

        return response.content.decode()

    def _reconnect_to_database(self) -> psycopg2._ext.connection:
        while True:
            try:
                return psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
            except psycopg2.OperationalError as e:
                self.log.warning('Exception during PostgreSQL connection: %s'.format(e))
                sleep(0.01)

    def _save_egrn_response(self, message: EgrnMessage, response: EgrnResponse) -> None:
        connection = self._reconnect_to_database()
        cursor = connection.cursor()
        self.log.info('PG connected')

        egrn_response_id = None
        try:
            self._execute_sql_query(
                cursor,
                """
                    INSERT INTO
                      egrn_response (cadastral_number, xml_id, pdf_id, region, address, area, okato, kladr, created_dt_egrn)
                    VALUES (%S, %S, %S, %S, %S, %S, %S, %S, %S)
                    RETURNING id
                """,
                (
                    message.cadastre_num, message.xml_id, message.pdf_id,
                    response.region, response.address, response.area,
                    response.okato, response.kladr, response.created_at
                )
            )
            egrn_response_id = cursor.fetchone()[0]

            for owner in response.owners:
                self._execute_sql_query(
                    cursor,
                    'INSERT INTO owner (egrn_id, owner_name, owner_type) VALUES (%S, %S, %S)',
                    (egrn_response_id, owner.owner_name, owner.owner_type)
                )

            for encumbrance in response.encumbrances:
                self._execute_sql_query(
                    cursor,
                    """
                        INSERT INTO
                          encumbrance (egrn_id, reg_number, reg_date, enc_type,
                          enc_name, enc_text, enc_started_at, enc_term,
                          owner_content, owner_name, owner_inn, owner_type)
                        VALUES (%S, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S)
                    """,
                    (
                        egrn_response_id, encumbrance.reg_number, encumbrance.reg_date,
                        encumbrance.encumbrance_type, encumbrance.name, encumbrance.text,
                        encumbrance.started_at, encumbrance.term, encumbrance.owner_content,
                        encumbrance.owner_name, encumbrance.owner_inn, encumbrance.owner_type
                    )
                )
        except Exception:
            ex_type, ex, tb = sys.exc_info()
            self._execute_sql_query(
                cursor,
                """
                    INSERT INTO
                      egrn_error(egrn_id, cadastral_number, xml_id, pdf_id, error_text, created_at)
                    VALUES (%S, %S, %S, %S, %S, %S)
                """,
                (
                    egrn_response_id, message.cadastre_num, message.xml_id, message.pdf_id,
                    traceback.format_tb(tb), response.created_at
                )
            )

        connection.commit()
        connection.close()

    def _execute_sql_query(self, cursor: psycopg2._ext.cursor, query: str, args: tuple) -> None:
        cursor.execute(query, args)
        self.log.info(query, *args)
