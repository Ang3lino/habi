
CREATE DATABASE IF NOT EXISTS actors;
ALTER DATABASE actors DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;

USE actors;

CREATE TABLE actor(
  id INT NOT NULL,
  actor_name VARCHAR(100),
  PRIMARY KEY(id)
);

CREATE TABLE actor_relation(
  id INT NOT NULL, 
  actor_id INT NOT NULL,
  PRIMARY KEY(id, actor_id),
  FOREIGN KEY(id) 
      REFERENCES actor(id) 
      ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY(actor_id)
      REFERENCES actor(id)
      ON DELETE CASCADE ON UPDATE CASCADE
);
