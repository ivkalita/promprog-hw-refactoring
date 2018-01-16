def rmq_callback(self, ch, method, properties, body):
    """
    Коллбэк для RabbitMQ. Парсит выписку, складывает получившееся в базу.
    Сообщение приходящее из очереди:
    {
      "success": "boolean",
      "messageId": "uuid",
      "data": {
         "xmlId": "long",
         "pdfId": "long",
         "cadastralNumber": "string",
         "egrnType": "enum[EGRN, EGRN_ARCHIVE]"
      }
    }
    xml_id, pdf_id, cadaster_number берутся из сообщения, остальное берется
    из XML.
    :param ch: pika required
    :type ch: pika channel
    :param method: pika required
    :type method:
    :param properties: pika required
    :type properties:
    :param body: pika required
    :type body: string
    :return: None
    :rtype: NoneType
    """
    self.log.info("Received %r" % body)
    json_response = json.loads(body.decode())
    xml_id = json_response['data'].get('xmlId')
    cad_num = json_response['data'].get('cadastralNumber')
    pdf_id = json_response['data'].get('pdfId')

    if not cad_num:
        self.log.info('No cadastralNumber in response')
        raise Exception('No cadastralNumber in response')
    if not xml_id:
        self.log.info('No XML ID for %s' % json_response.get('cadastralNumber'))
        raise Exception(
            'No XML ID for %s' % json_response.get('cadastralNumber'))
    self.log.debug(cad_num, xml_id)

    resp = requests.get(STORAGE + str(xml_id))
    self.log.info("Downloaded binary for %s", xml_id)
    if resp.status_code != 200:
        self.log.debug("Cannot download binary %s", xml_id)
        raise Exception("Cannot download binary")
    if 'text' in resp.headers['Content-Type'] or 'json' in resp.headers[
        'Content-Type']:
        self.log.debug("Cannot download binary, probably 404")
        raise Exception("Cannot download binary, probably 404")
    xml_string = resp.content.decode()
    self.log.info("Downloaded binary BINBEGIN %s BINEND", xml_string)

    while True:
        try:
            pg_connection = psycopg2.connect(dbname=DB_NAME,
                                             user=DB_USER,
                                             password=DB_PASSWORD,
                                             host=DB_HOST,
                                             port=DB_PORT)
            pg_cursor = pg_connection.cursor()
            break
        except Exception as exc:
            self.log.info('Exception while connecting to pg %s', exc)
    self.log.info('PG connected')
    egrn_response_id = None
    struct = {}
    try:
        struct = parse_string(xml_string)
        sql = """INSERT INTO egrn_response (cadastral_number, xml_id, pdf_id, region, address, area, okato, kladr, created_dt_egrn)
                    VALUES (%S, %S, %S, %S, %S, %S, %S, %S, %S) RETURNING id"""
        sql_args = (cad_num, xml_id, pdf_id,
                    struct['region'], struct['address'], struct['area'],
                    struct['okato'], struct['kladr'], struct['created_dt_egrn'])
        self.log.info(sql, *sql_args)
        pg_cursor.execute(sql, sql_args)
        egrn_response_id = pg_cursor.fetchone()[0]

        for ownr in struct['owners']:
            sql = """INSERT INTO owner (egrn_id, owner_name, owner_type) 
                        VALUES (%S, %S, %S)"""
            sql_args = (
                egrn_response_id, ownr['owner_name'], ownr['owner_type'])
            pg_cursor.execute(sql, sql_args)
            self.log.info(sql, *sql_args)

        for enc in struct['encs']:
            sql = """INSERT INTO encumbrance (egrn_id, reg_number, 
               reg_date, enc_type, enc_name, enc_text, enc_started_dt, 
               enc_term, owner_content, owner_name, owner_inn, owner_type)
                        VALUES (%S, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S)"""
            sql_args = (
                egrn_response_id, enc['reg_number'], enc['reg_date'],
                enc['enc_type'], enc['enc_name'], enc['enc_text'],
                enc['enc_started_dt'], enc['enc_term'],
                enc.get('owner_content'), enc.get('owner_name'),
                enc.get('owner_inn'), enc.get('owner_type'))
            pg_cursor.execute(sql, *sql_args)
            self.log.info(sql, sql_args)

    except Exception as exc:
        ex_type, ex, tb = sys.exc_info()
        pg_cursor.execute(
            """INSERT INTO egrn_error (egrn_id, cadastral_number, 
            xml_id, pdf_id, error_text, created_at)
               VALUES (%S, %S, %S, %S, %S, %S) RETURNING id""",
            (egrn_response_id, cad_num, xml_id, pdf_id,
             traceback.format_tb(tb),
             struct.get('created_dt_egrn')))
    pg_connection.commit()
    pg_connection.close()