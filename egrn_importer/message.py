import json
from typing import Optional

import jsonschema

from egrn_importer.constants import EgrnType


class MessageParseException(BaseException):
    def __init__(self, message: str, error: BaseException) -> None:
        self.message = message
        self.error = error

    def __str__(self) -> str:
        return 'MessageParseException(message={}, error={})'.format(self.message, self.error)


class EgrnMessage:
    SCHEMA = {
        'type': 'object',
        'properties': {
            'success': {'type': 'boolean'},
            'uuid': {'type': 'string'},
            'data': {
                'type': 'object',
                'properties': {
                    'xmlId': {'type': 'number'},
                    'pdfId': {'type': ['number', 'null']},
                    'cadastralNumber': {'type': ['string', 'null']},
                    'egrnType': {'type': 'string'}
                },
                'required': ['xmlId', 'pdfId', 'cadastractNumber', 'egrnType']
            },
            'required': ['success', 'uuid', 'data']
        }
    }

    def __init__(self, success: bool, message_id: str, xml_id: int, cadastre_num: str, egrn_type: EgrnType,
                 pdf_id: Optional[int] = None) -> None:
        self.success = success
        self.message_id = message_id
        self.xml_id = xml_id
        self.cadastre_num = cadastre_num
        self.egrn_type = egrn_type
        self.pdf_id = pdf_id

    @staticmethod
    def from_str(body: str) -> 'EgrnMessage':
        try:
            parsed = json.loads(body)
            jsonschema.validate(parsed, schema=EgrnMessage.SCHEMA)
            return EgrnMessage(
                success=parsed['success'],
                message_id=parsed['uuid'],
                xml_id=int(parsed['data']['xml_id']),
                cadastre_num=parsed['data']['cadastralNumber'],
                egrn_type=EgrnType(parsed['data']['egrnType']),
                pdf_id=int(parsed['data']['pdfId']) if parsed.data['pdfId'] is not None else None
            )
        except (ValueError, TypeError, jsonschema.ValidationError) as e:
            raise MessageParseException(body, e)

    def __str__(self) -> str:
        return 'EgrnMessage(cadastre_num={}, xml_id={})'.format(self.cadastre_num, self.xml_id)
