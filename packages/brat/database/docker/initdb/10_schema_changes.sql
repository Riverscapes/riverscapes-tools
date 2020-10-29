ALTER TABLE watersheds ADD COLUMN created_on timestamptz NOT NULL DEFAULT now();
ALTER TABLE watersheds ADD COLUMN updated_on timestamptz NOT NULL DEFAULT now();
ALTER TABLE vegetation_types ADD COLUMN created_on timestamptz NOT NULL DEFAULT now();
ALTER TABLE vegetation_types ADD COLUMN updated_on timestamptz NOT NULL DEFAULT now();
ALTER TABLE hydro_params ADD COLUMN created_on timestamptz NOT NULL DEFAULT now();
ALTER TABLE hydro_params ADD COLUMN updated_on timestamptz NOT NULL DEFAULT now();
ALTER TABLE watershed_hydro_params ADD COLUMN created_on timestamptz NOT NULL DEFAULT now();
ALTER TABLE watershed_hydro_params ADD COLUMN updated_on timestamptz NOT NULL DEFAULT now();
ALTER TABLE vegetation_overrides ADD COLUMN created_on timestamptz NOT NULL DEFAULT now();
ALTER TABLE vegetation_overrides ADD COLUMN updated_on timestamptz NOT NULL DEFAULT now();

CREATE OR REPLACE FUNCTION befo_update()
RETURNS  trigger AS
    $$
    BEGIN
        NEW.updated_on = now();
        RETURN NEW;
    END ;
$$
LANGUAGE 'plpgsql';

CREATE TRIGGER tr_watersheds_update BEFORE UPDATE ON watersheds
    FOR EACH ROW
    EXECUTE PROCEDURE befo_update();

CREATE TRIGGER tr_vegetation_types BEFORE UPDATE ON vegetation_types
    FOR EACH ROW
    EXECUTE PROCEDURE befo_update();

CREATE TRIGGER tr_hydro_params BEFORE UPDATE ON hydro_params
    FOR EACH ROW
    EXECUTE PROCEDURE befo_update();

CREATE TRIGGER tr_watershed_hydro_params BEFORE UPDATE ON watershed_hydro_params
    FOR EACH ROW
    EXECUTE PROCEDURE befo_update();

CREATE TRIGGER tr_vegetation_overrides BEFORE UPDATE ON vegetation_overrides
    FOR EACH ROW
    EXECUTE PROCEDURE befo_update();

