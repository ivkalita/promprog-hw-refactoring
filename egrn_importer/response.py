from typing import List


class EgrnOwner:
    def __init__(self, **kwargs: dict) -> None:
        self.owner_name = kwargs.get('owner_name')
        self.owner_type = kwargs.get('owner_type')


class EgrnEncumbrance:
    def __init__(self, **kwargs) -> None:
        self.reg_number = kwargs.get('reg_number')
        self.reg_date = kwargs.get('reg_date')
        self.encumbrance_type = kwargs.get('encumbrance_type')
        self.name = kwargs.get('name')
        self.text = kwargs.get('text')
        self.started_at = kwargs.get('started_at')
        self.term = kwargs.get('term')
        self.owner_content = kwargs.get('owner_content')
        self.owner_inn = kwargs.get('owner_inn')
        self.owner_name = kwargs.get('owner_name')
        self.owner_type = kwargs.get('owner_type')


class EgrnResponse:
    def __init__(self, **kwargs) -> None:
        self.region = kwargs.get('region')
        self.address = kwargs.get('address')
        self.area = kwargs.get('area')
        self.okato = kwargs.get('okato')
        self.kladr = kwargs.get('kladr')
        self.created_at = kwargs.get('created_at')
        self.encumbrances: List[EgrnEncumbrance] = []
        self.owners: List[EgrnOwner] = []

    def _add_encumbrance(self, encumbrance: EgrnEncumbrance) -> 'EgrnResponse':
        self.encumbrances.append(encumbrance)
        return self

    def _add_owner(self, owner: EgrnOwner) -> 'EgrnResponse':
        self.owners.append(owner)
        return self

    @staticmethod
    def from_str(xml_string: str) -> 'EgrnResponse':
        # Тут по идее находится парсинг XML строки с валидациями по типу как сделано
        # в egrn_importer.message. Для простоты (и лень возиться с валидаторами XML)
        # опускаю этот код и вместо этого возвращаю некоторый заранее известный объект
        # в нужной структуре.
        response = EgrnResponse(
            region='1',
            address='2',
            area='3',
            okato='4',
            kladr='5',
            created_at='6'
        )
        return response

    def __str__(self) -> str:
        return 'EgrnResponse(region={}, okato={}, address={})'.format(
            self.region, self.okato, self.address
        )
