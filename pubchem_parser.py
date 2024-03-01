import re
import sqlite3 as sq
from dataclasses import dataclass
from typing import Tuple, List

import pubchempy as pcp


db_name = 'pubchem.db'


@dataclass
class Compound:
    cid: int
    smiles: str
    iupac_name: str
    trivial_names: Tuple[str]


class Database:

    """
    Database structure:
        1. Table Compounds: CID (pk), IUPAC, Smiles
        2. Table TrivialNames: ID (pk, i++), Name, CID (CID from table Compounds)
        3. Table CIDNameMatches: ID(pk, i++), CID (CID from table Compounds), NameID (ID from table TrivialNames)
    """

    db_name = db_name

    @staticmethod
    def create() -> None:

        with sq.connect(Database.db_name) as con:
            cur = con.cursor()

            cur.execute("""create table if not exists Compounds(
                               CID integer primary key,
                               IUPAC string,
                               Smiles string);
                        """)

            cur.execute("""create table if not exists TrivialNames(
                               ID integer primary key autoincrement,
                               Name string);
                        """)

            cur.execute("""create table if not exists CIDNameMatches(
                                           ID integer primary key autoincrement,
                                           NameID integer,
                                           CID integer);
                                    """)

    @staticmethod
    def get_max_cid() -> int | None:

        with sq.connect(Database.db_name) as con:
            cur = con.cursor()
            max_cid = cur.execute("SELECT MAX(CID) FROM Compounds;").fetchone()[0]

        return max_cid

    @staticmethod
    def trivial_name_exists(name: str) -> bool:

        with sq.connect(Database.db_name) as con:
            cur = con.cursor()
            trivial_name_found = cur.execute(f"""SELECT ID FROM TrivialNames WHERE Name == "{name}";""").fetchone()
            trivial_name_found = True if trivial_name_found else False

        return trivial_name_found

    @staticmethod
    def cid_exists(cid: int) -> bool:
        with sq.connect(Database.db_name) as con:
            cur = con.cursor()
            cid_found = cur.execute(f"SELECT CID FROM Compounds WHERE CID == {cid};").fetchone()
            cid_found = True if cid_found else False

        return cid_found

    @staticmethod
    def get_trivial_name_id(name: str):

        with sq.connect(Database.db_name) as con:
            cur = con.cursor()
            name_id = cur.execute(f"""SELECT ID FROM TrivialNames WHERE Name == "{name}";""").fetchone()
            name_id = name_id[0] if name_id else None

        return name_id

    @staticmethod
    def insert_trivial_name(name: str) -> None:

        with sq.connect(Database.db_name) as con:
            cur = con.cursor()
            cur.execute(f"""INSERT INTO TrivialNames(Name) VALUES("{name}");""")

    @staticmethod
    def insert_cid_name_match(cid: int, name_id: int) -> None:

        with sq.connect(Database.db_name) as con:
            cur = con.cursor()
            cur.execute(f"INSERT INTO CIDNameMatches(CID, NameID) VALUES({cid}, {name_id});")

    @staticmethod
    def insert_compound(compound: Compound) -> None:

        with sq.connect(Database.db_name) as con:
            cur = con.cursor()
            cur.execute(f"""INSERT INTO Compounds(CID, IUPAC, Smiles)
                        VALUES({compound.cid}, "{compound.iupac_name}", "{compound.smiles}");""")

    @staticmethod
    def add_trivial_name(name: str, cid: int) -> bool:

        if not Database.trivial_name_exists(name=name):
            Database.insert_trivial_name(name=name)
            inserted = True
        else:
            inserted = False

        name_id = Database.get_trivial_name_id(name=name)
        if name:
            Database.insert_cid_name_match(cid=cid, name_id=name_id)

        return inserted

    @staticmethod
    def add_compound(compound: Compound) -> bool:

        if Database.cid_exists(cid=compound.cid):
            return False

        Database.insert_compound(compound)

        for trivial_name in compound.trivial_names:
            Database.add_trivial_name(name=trivial_name, cid=compound.cid)

        return True


def filter_names(names: Tuple[str]) -> List[str]:

    filtered_names = [name for name in names if isinstance(name, str)]

    pattern = re.compile(r'(\d{2,}|, )')
    filtered_names = [s for s in filtered_names if not pattern.search(s)]

    for i, name in enumerate(filtered_names):
        if name.startswith('"') and name.endswith('"'):
            filtered_names[i] = name[1:-1]

    return filtered_names


def get_compound(cid: int) -> Compound:

    compound_obj = None
    try:
        compound = pcp.Compound.from_cid(cid)
        smiles = compound.canonical_smiles
        iupac_name = compound.iupac_name
        trivial_names = compound.synonyms

        trivial_names = filter_names(trivial_names)
        trivial_names = tuple(set(trivial_names))

        if smiles and iupac_name:
            compound_obj = Compound(
                cid=cid,
                smiles=smiles,
                iupac_name=iupac_name,
                trivial_names=trivial_names
            )

    except Exception as ex:
        print(f"Error fetching compound with CID {cid}: {ex}")

    return compound_obj


def run_cids(count: int = 50) -> None:

    print('Run CIDs starting...')

    Database.create()
    max_cid = Database.get_max_cid()
    start_cid = max_cid + 1 if max_cid else 1

    print(f'From: {start_cid}, Count: {count}', end='\n\n')

    broken_cids = []

    for cid in range(start_cid, start_cid+count):

        compound = get_compound(cid=cid)

        resp = False
        if compound:
            try:
                resp = Database.add_compound(compound)
            except Exception as ex:
                log = f"Error during writing compound to database. Exception: {ex}"
                print('\033[91m' + log + '\033[0m')

        log = f'CID {cid}'
        if resp:
            log += '  \033[92m' + 'OK' + '\033[0m'
        else:
            log += '  \033[93m' + 'was not added' + '\033[0m'
            broken_cids.append(cid)

        print(log)

    broken_cids_count = len(broken_cids)
    print("\nRun CIDs finished")
    report_line = f'OK: \033[92m{count-broken_cids_count}\033[0m, '
    if broken_cids_count:
        report_line += f'Not added: \033[91m{broken_cids_count}\033[0m'
    else:
        report_line += f'Not added: \033[92m{broken_cids_count}\033[0m'
    print(report_line)
    if broken_cids_count:
        print(f'\nBroken CIDs:\n{broken_cids}')


if __name__ == '__main__':
    run_cids(count=100)
