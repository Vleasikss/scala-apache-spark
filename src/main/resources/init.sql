DROP DATABASE IF EXISTS apache_spark_example;
CREATE DATABASE apache_spark_example;
CREATE TABLE IF NOT EXISTS user
(
    `id`   BIGINT      NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(64) NOT NULL,
    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    AUTO_INCREMENT = 3
    DEFAULT CHARACTER SET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci;

INSERT INTO user
values (1, 'Vityok');
INSERT INTO user
VALUES (2, 'Olega')