DROP TABLE IF EXISTS `kafka_user`;
CREATE TABLE `kafka_user` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `age` int(11) NOT NULL,
  `gender` varchar(10) NOT NULL,
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;