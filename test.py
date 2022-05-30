

INSERT_SQL = """
INSERT INTO submit_log (msgid, source_addr, rate, pdu_count,
                        destination_addr, short_message,
                        status, uid, created_at, binary_message,
                        routed_cid, source_connector, status_at, trials) VALUES
('0f897934-0a5e-4c50-89a7-69d3bc96ee05', 'smppsapi', 
'idealcom', '\x57455a4154454348', '\x323534373232393031323932', 0.00, 1, '\x546573742035', '\x353436353733373432303335', 
'ESME_ROK', 'ITC', null, '2022-05-30 17:09:44.764918+00', '2022-05-30 17:09:44.764918+00', 1)
"""