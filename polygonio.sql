-- --------------------------------------------------------
-- Host:                         Station26
-- Server version:               10.5.7-MariaDB - mariadb.org binary distribution
-- Server OS:                    Win64
-- HeidiSQL Version:             11.0.0.5919
-- --------------------------------------------------------

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8 */;
/*!50503 SET NAMES utf8mb4 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;


-- Dumping database structure for networkeasy
CREATE DATABASE IF NOT EXISTS `networkeasy` /*!40100 DEFAULT CHARACTER SET utf8 */;
USE `networkeasy`;

-- Dumping structure for table networkeasy.currentdaycalc
CREATE TABLE IF NOT EXISTS `currentdaycalc` (
  `s` datetime DEFAULT NULL,
  `o` double DEFAULT NULL,
  `h` double DEFAULT NULL,
  `l` double DEFAULT NULL,
  `c` double DEFAULT NULL,
  `sym` varchar(50) DEFAULT NULL,
  `EMA12` double DEFAULT NULL,
  `EMA26` double DEFAULT NULL,
  `MACD` double DEFAULT NULL,
  `Sig9` double DEFAULT NULL,
  `Diff` double DEFAULT NULL,
  `RSI` double DEFAULT NULL,
  `BBandUp` double DEFAULT NULL,
  `BBandDown` double DEFAULT NULL,
  `BBandBasis` double DEFAULT NULL,
  `TR` double DEFAULT NULL,
  `ATR` double DEFAULT NULL,
  `RSIOVERLINE` double DEFAULT NULL,
  `TR14` double DEFAULT NULL,
  `PDMI14` double DEFAULT NULL,
  `NDMI14` double DEFAULT NULL,
  `PDI14` double DEFAULT NULL,
  `NDI14` double DEFAULT NULL,
  `DI14Diff` double DEFAULT NULL,
  `DI14Sum` double DEFAULT NULL,
  `DX` double DEFAULT NULL,
  `ADX` double DEFAULT NULL,
  KEY `ticker` (`sym`,`s`) USING BTREE,
  KEY `date` (`s`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Dumping data for table networkeasy.currentdaycalc: ~1,561 rows (approximately)
/*!40000 ALTER TABLE `currentdaycalc` DISABLE KEYS */;
/*!40000 ALTER TABLE `currentdaycalc` ENABLE KEYS */;

-- Dumping structure for table networkeasy.currentdayraw
CREATE TABLE IF NOT EXISTS `currentdayraw` (
  `s` datetime DEFAULT NULL,
  `o` double DEFAULT NULL,
  `h` double DEFAULT NULL,
  `l` double DEFAULT NULL,
  `c` double DEFAULT NULL,
  `sym` varchar(50) DEFAULT NULL,
  KEY `name` (`sym`) USING BTREE,
  KEY `date` (`s`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Dumping data for table networkeasy.currentdayraw: ~1,561 rows (approximately)
/*!40000 ALTER TABLE `currentdayraw` DISABLE KEYS */;
INSERT INTO `currentdayraw` (`s`, `o`, `h`, `l`, `c`, `sym`) VALUES
	('2021-01-22 21:00:01', 47.43, 47.43, 47.43, 47.43, 'XOM'),
	('2021-01-22 21:00:02', 47.43, 47.43, 47.43, 47.43, 'XOM'),
	('2021-01-22 21:00:07', 47.43, 47.43, 47.43, 47.43, 'XOM'),
	('2021-01-22 21:00:11', 47.43, 47.43, 47.43, 47.43, 'XOM');
/*!40000 ALTER TABLE `currentdayraw` ENABLE KEYS */;

/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IF(@OLD_FOREIGN_KEY_CHECKS IS NULL, 1, @OLD_FOREIGN_KEY_CHECKS) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
