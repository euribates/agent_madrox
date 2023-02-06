CREATE OR REPLACE TRIGGER CREAR_JORNADAS
AFTER INSERT ON SESION
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
DECLARE existen NUMBER;
        SALA_DEF NUMBER;
        DESCRIP VARCHAR2(512);
begin
  -- Si ya existe la jornada, no hay que hacer nada
  select COUNT(*) into EXISTEN from AGORA.JORNADA J where J.ID_SESION = :NEW.ID_SESION;
  IF (EXISTEN > 0) THEN
    return;
  END IF;

  SALA_DEF := 0;
  select COUNT(*) into EXISTEN from AGORA.SALA_PRESELE SP where SP.ID_ORGANO=:NEW.ID_ORGANO;
  IF EXISTEN>0 THEN
        select ID_SALA into SALA_DEF from AGORA.SALA_PRESELE SP where SP.ID_ORGANO=:NEW.ID_ORGANO;
  ELSE
        SALA_DEF:= 0;
  END IF;
--  exception
--     when NO_DATA_FOUND THEN
--     SALA_DEF:= 1;


  select NOMBRE  into DESCRIP  from AGORA.ORGANO O        WHERE O.ID_ORGANO= :NEW.ID_ORGANO;
  IF (substr(:new.ID_ORGANO, 4, 3) = '203') THEN  -- Es un pleno

      INSERT INTO AGORA.JORNADA
                         (id_jornada,id_sesion, id_organo, fecha, hora, descripcion, id_sala, canal)
      VALUES( AGORA.SEQ_JORNADA.NEXTVAL, :NEW.ID_SESION, :NEW.ID_ORGANO ,
             :NEW.FECHA, :NEW.HORA, DESCRIP, SALA_DEF, 1 );

      INSERT INTO AGORA.JORNADA
                         (id_jornada,id_sesion, id_organo, fecha, hora, descripcion, id_sala, canal)
      VALUES( AGORA.SEQ_JORNADA.NEXTVAL ,:NEW.ID_SESION, :NEW.ID_ORGANO ,
             :NEW.FECHA +1 , '09:00', DESCRIP || ' (continuación)', SALA_DEF, 1 );


  ELSE  -- Es una reunion ordinaria

      INSERT INTO AGORA.JORNADA
                         (id_jornada,id_sesion, id_organo, fecha, hora, descripcion, id_sala, canal)
      VALUES( AGORA.SEQ_JORNADA.NEXTVAL, :NEW.ID_SESION, :NEW.ID_ORGANO ,
             :NEW.FECHA, :NEW.HORA, DESCRIP, SALA_DEF, 0 );

  END IF;


END;
