
--
-- Table structure for table `budget_unit_joins`
--

DROP TABLE IF EXISTS `budget_unit_joins`;
CREATE TABLE `budget_unit_joins` (
  `unit_pk` int NOT NULL,
  `contract_pk` int NOT NULL,
  PRIMARY KEY (`unit_pk`,`contract_pk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Table structure for table `budget_unit_names`
--

DROP TABLE IF EXISTS `budget_unit_names`;
CREATE TABLE `budget_unit_names` (
  `unit_pk` int DEFAULT NULL,
  `unit_num` int DEFAULT NULL,
  `unit_name` varchar(255) DEFAULT NULL,
  UNIQUE KEY `unit_pk` (`unit_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Table structure for table `budget_units`
--

DROP TABLE IF EXISTS `budget_units`;
CREATE TABLE `budget_units` (
  `pk` int NOT NULL,
  `unit_num` int DEFAULT NULL,
  `unit_name` varchar(127) DEFAULT NULL,
  PRIMARY KEY (`pk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Table structure for table `contracts`
--

DROP TABLE IF EXISTS `contracts`;
CREATE TABLE `contracts` (
  `pk` int NOT NULL,
  `owner_name` varchar(63) DEFAULT NULL,
  `ariba_id` varchar(63) DEFAULT NULL,
  `sap_id` varchar(63) DEFAULT NULL,
  `contract_id` varchar(63) DEFAULT NULL,
  `contract_type` char(2) DEFAULT NULL,
  `vendor_name` varchar(127) DEFAULT NULL,
  `vendor_pk` int DEFAULT NULL,
  `effective_date` char(10) DEFAULT NULL,
  `expir_date` char(10) DEFAULT NULL,
  `contract_value` bigint DEFAULT NULL,
  `commodity_desc` varchar(1027) DEFAULT NULL,
  `month_pk` int DEFAULT NULL,
  `source_pk` int DEFAULT NULL,
  `line_num` int DEFAULT NULL,
  PRIMARY KEY (`pk`),
  KEY `month_pk` (`month_pk`),
  KEY `vendor_name` (`vendor_name`),
  KEY `vendor_pk` (`vendor_pk`),
  KEY `contract_value` (`contract_value`),
  KEY `ariba_id` (`ariba_id`,`sap_id`,`contract_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Table structure for table `months`
--

DROP TABLE IF EXISTS `months`;
CREATE TABLE `months` (
  `pk` int NOT NULL,
  `month` char(7) DEFAULT NULL,
  `approved` bigint DEFAULT NULL,
  PRIMARY KEY (`pk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Table structure for table `sources`
--

DROP TABLE IF EXISTS `sources`;
CREATE TABLE `sources` (
  `pk` int NOT NULL,
  `source_url` varchar(511) DEFAULT NULL,
  `alternate_url` varchar(511) DEFAULT NULL,
  `month_pk` int DEFAULT NULL,
  PRIMARY KEY (`pk`),
  KEY `month_pk` (`month_pk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Table structure for table `supporting_doc_requests`
--

DROP TABLE IF EXISTS `supporting_doc_requests`;
CREATE TABLE `supporting_doc_requests` (
  `pk` int NOT NULL,
  `ariba_id` varchar(63) DEFAULT NULL,
  `contract_id` varchar(63) DEFAULT NULL,
  `sap_id` varchar(63) DEFAULT NULL,
  `request_entity` varchar(63) DEFAULT NULL,
  `requested` char(10) DEFAULT NULL,
  PRIMARY KEY (`pk`),
  UNIQUE KEY `ariba_id` (`ariba_id`,`contract_id`,`sap_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Table structure for table `supporting_docs`
--

DROP TABLE IF EXISTS `supporting_docs`;
CREATE TABLE `supporting_docs` (
  `pk` int NOT NULL,
  `request_pk` int DEFAULT NULL,
  `url` varchar(1023) DEFAULT NULL,
  `received` char(10) DEFAULT NULL,
  PRIMARY KEY (`pk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Table structure for table `vendor_infos`
--

DROP TABLE IF EXISTS `vendor_infos`;
CREATE TABLE `vendor_infos` (
  `pk` int NOT NULL,
  `vendor_pk` int DEFAULT NULL,
  `key_name` varchar(63) DEFAULT NULL,
  `value_str` varchar(1023) DEFAULT NULL,
  `created` bigint DEFAULT NULL,
  `updated` int DEFAULT NULL,
  PRIMARY KEY (`pk`),
  KEY `vendor_pk` (`vendor_pk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Table structure for table `vendor_names`
--

DROP TABLE IF EXISTS `vendor_names`;
CREATE TABLE `vendor_names` (
  `vendor_pk` int DEFAULT NULL,
  `name` varchar(127) DEFAULT NULL,
  UNIQUE KEY `vendor_pk` (`vendor_pk`,`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Table structure for table `vendors`
--

DROP TABLE IF EXISTS `vendors`;
CREATE TABLE `vendors` (
  `pk` int NOT NULL,
  `name` varchar(127) DEFAULT NULL,
  PRIMARY KEY (`pk`),
  UNIQUE KEY `name` (`name`),
  KEY `name_2` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
