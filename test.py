

INSERT_SQL = """
INSERT INTO submit_log 
(msgid, source_connector, routed_cid, 
	source_addr, destination_addr, rate, pdu_count, 
	short_message, binary_message, status, uid, trials, created_at, status_at)
VALUES
('0f897934-0a5e-4c50-89a7-69d3bc96ee05', 'smppsapi', 'idealcom', 
	'\x57455a4154454348', '\x323534373232393031323932', 0.00, 1, 
	'\x546573742035', '\x353436353733373432303335', 'ESME_ROK', 'ITC', 1, '2022-05-30 17:09:44.764918+00', '2022-05-30 17:09:44.764918+00')
"""