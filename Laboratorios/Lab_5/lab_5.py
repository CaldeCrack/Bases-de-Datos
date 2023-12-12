import psycopg2, csv, re

# Accedemos a la base de datos
conn = psycopg2.connect(host ="cc3201.dcc.uchile.cl",
    database ="cc3201",
    user ="cc3201",
    password ="j'<3_cc3201",
    port ="5440")

cur = conn.cursor()

# abrir schema superheroes si es que es necesario
cur.execute("set search_path to superheroes;")

# función auxiliar para encontrar o insertar un valor en una tabla
def findOrInsert(table, name):
    cur.execute(f"select id from {table} where name = %s limit 1;", [name])
    r = cur.fetchone()
    if r:
        return r[0]
    cur.execute(f"insert into {table} (name) values (%s) returning id;", [name])
    return cur.fetchone()[0]

# (a) Creación del superhéroe
with open("Laboratorio_05_data.csv") as file:
    reader = csv.reader(file, delimiter=",", quotechar='"')
    i = -1
    for row in reader:
        # saltarse el encabezado
        i += 1
        if i == 0:
            continue

        # i. personajes
        name = row[8]
        if name:
            _id = findOrInsert("FAC_Character", name)
        else:
            name = row[1]
            _id = findOrInsert("FAC_Character", name)

        # ii. superhéroes
        cur.execute("select id from FAC_Superheroe where id = %s limit 1;", [_id])
        if not cur.fetchone():
            name = row[1]
            intelligence = row[2] if row[2] != "null" else None
            strength = row[3] if row[3] != "null" else None
            speed = row[4] if row[4] != "null" else None
            cur.execute("insert into FAC_Superheroe (id, name, intelligence, strength, speed) values (%s, %s, %s, %s, %s);",
                        [_id, name, intelligence, strength, speed])

        # iii. alter egos
        alter_egos = row[9]
        alter_egos = [alter_ego.strip() for alter_ego in re.split("[,;]", alter_egos.replace("\"", ""))]
        if alter_egos != ["No alter egos found."]:
            for alter_ego in alter_egos:
                id_alter = findOrInsert("FAC_Alterego", alter_ego)
                cur.execute("select id_sh, id_alter from FAC_Superheroe_Alterego where id_sh = %s and id_alter = %s;", [_id, id_alter])
                if not cur.fetchone():
                    cur.execute("insert into FAC_Superheroe_Alterego (id_sh, id_alter) values (%s, %s);", [_id, id_alter])

        # iv. ocupaciones
        ocupaciones = row[23]
        ocupaciones = [ocupacion.strip().lower() for ocupacion in re.split("[,;]", ocupaciones.replace("\"", ""))]
        if ocupaciones != ["-"]:
            for ocupacion in ocupaciones:
                id_wo = findOrInsert("FAC_WorkOcupation", ocupacion)
                cur.execute("select id_sh, id_wo from FAC_Superheroe_WorkOcupation where id_sh = %s and id_wo = %s;", [_id, id_wo])
                if not cur.fetchone():
                    cur.execute("insert into FAC_Superheroe_WorkOcupation (id_sh, id_wo) values (%s, %s);", [_id, id_wo])

# (b) Creación de los parientes
with open("Laboratorio_05_data.csv") as file:
    reader = csv.reader(file, delimiter=",", quotechar='"')
    i = -1
    for row in reader:
        # saltarse el encabezado
        i += 1
        if i == 0:
            continue

        # i. id personaje
        name = row[8]
        if name:
            _id = findOrInsert("FAC_Character", name)
        else:
            name = row[1]
            _id = findOrInsert("FAC_Character", name)

        # ii. ignorar filas con "-"
        relaciones = row[26]
        if relaciones != ["-"]:
            # iii. relaciones
            relaciones = [relacion.strip() for relacion in re.split("[,;]", relaciones.replace("\"", ""))]
            for relacion in relaciones:
                m = re.search("([^(]+)[ ]*\(([^)]+)\)", relacion)
                if not m:
                    continue
                pariente_nombre = m.group(1).strip()
                pariente_relacion = m.group(2).strip()
                cur.execute("select id from FAC_Character where name = %s;", [pariente_nombre])
                pariente_id = cur.fetchone()
                if not pariente_id:
                    cur.execute("select id from FAC_Superheroe where name = %s;", [pariente_nombre])
                    pariente_id = cur.fetchone()
                if not pariente_id:
                    cur.execute("insert into FAC_Character (name) values (%s) returning id;", [pariente_nombre])
                    pariente_id = cur.fetchone()[0]
                relacion_id = findOrInsert("FAC_Relation", pariente_relacion)
                cur.execute("select * from FAC_Character_Superheroe_Relation where id_ch1 = %s and id_ch2 = %s and id_rel = %s;", [_id, pariente_id, relacion_id])
                if not cur.fetchone():
                    cur.execute("insert into FAC_Character_Superheroe_Relation (id_ch1, id_ch2, id_rel) values (%s, %s, %s);", [_id, pariente_id, relacion_id])

# (c) commit de los cambios
conn.commit()
conn.close()