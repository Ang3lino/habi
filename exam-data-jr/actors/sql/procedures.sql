USE actors;


DROP PROCEDURE IF EXISTS usp_actor_add;
DELIMITER &
CREATE PROCEDURE usp_actor_add(
    IN p_id INT,
    IN p_actor_name VARCHAR(100)
) BEGIN 
    INSERT INTO actor(id, actor_name)
        VALUES (p_id, p_actor_name) 
    ;
    END &
DELIMITER ;

DROP PROCEDURE IF EXISTS usp_actor_relation_add;
DELIMITER &
CREATE PROCEDURE usp_actor_relation_add(
    IN p_id INT,
    IN p_actor_id INT
) BEGIN 
    INSERT INTO actor_relation(id, actor_id)
        VALUES (p_id, p_actor_id) 
    ;
    END &
DELIMITER ;


