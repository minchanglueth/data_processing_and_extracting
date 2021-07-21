# import _locate_
# import sys

# sys.path.insert(1, _locate_.BASE_DIR)
from connect_confidential.connect_db_final import conn, cursor

# from connect_confidential.connect_db_stg import conn, cursor
from connect_confidential.connect_gspread import client_gspread

import pandas as pd
from colorama import Fore, Style


def check_pointlog(check_query, actionid, reset_crawlingtaskid):
    query = check_query.format(actionid, reset_crawlingtaskid)  # album_query
    cursor.execute(query)
    result = cursor.fetchall()
    pointlog_list = pd.DataFrame(result)[0].to_list()
    return pointlog_list


def update_db(query):
    print(
        Fore.LIGHTBLUE_EX + "\nPlease check if the query is correct" + Style.RESET_ALL
    )
    print(query)
    decision = input(
        Fore.LIGHTMAGENTA_EX + "\nEnter RESET to continue: " + Style.RESET_ALL
    )
    if decision == "RESET":
        cursor.execute(query)
        conn.commit()
        print(Fore.LIGHTYELLOW_EX + "Finish reset crawlingtask!")
        print(
            "If later the crawlingtask's result still returns incomplete please inform Minchan"
            + Style.RESET_ALL
        )
    else:
        print("No changes made, now exit")


rerun_crawlingtask = """UPDATE v4.crawlingtasks set `Status` = NULL where `Status` = 'incomplete' and actionid ='{}' and id in {};"""
valid_plid_query = """select id from v4.crawlingtasks where `Status` = 'incomplete' and actionid ='{}' and id in {}"""


def check_update(reset_crawlingtaskid, actionid):
    valid_plid = check_pointlog(
        valid_plid_query,
        actionid,
        str(reset_crawlingtaskid).replace("[", "(").replace("]", ")"),
    )
    invalid_plid = [i for i in reset_crawlingtaskid if i not in valid_plid]
    print(
        Fore.LIGHTGREEN_EX
        + "\nvalid crawlingtasks.id list as below:\n"
        + Style.RESET_ALL,
        valid_plid,
    )
    print(
        Fore.LIGHTRED_EX
        + "\ninvalid crawlingtasks.id list as below:\n"
        + Style.RESET_ALL,
        invalid_plid,
    )
    if len(invalid_plid) == 0:
        rerun_query = rerun_crawlingtask.format(
            actionid, str(valid_plid).replace("[", "(").replace("]", ")")
        )
        update_db(rerun_query)
    else:
        print(
            Fore.LIGHTRED_EX
            + "Please recheck INVALID crawlingtasks.id"
            + Style.RESET_ALL
        )


def reset_crawlings(reset_crawlingtaskid):
    print(
        Fore.LIGHTGREEN_EX + "\nWhich ActionID you want to reset?\n",
        "A. youtube-crawler_FFC1\n",
        "B. album-itune_FC06\n",
        "C. album-tracks_0DE5\n" + Style.RESET_ALL,
    )
    option = str(
        input(Fore.LIGHTMAGENTA_EX + "Please input the option: " + Style.RESET_ALL)
    )

    if option == "A":
        check_update(reset_crawlingtaskid, "F91244676ACD47BD9A9048CF2BA3FFC1")
    elif option == "B":
        check_update(reset_crawlingtaskid, "9C8473C36E57472281A1C7936108FC06")
    elif option == "C":
        check_update(reset_crawlingtaskid, "0001892134894FCF83ABCFC1F39E0DE5")
    else:
        print("No changes made, now exit")
    print(Fore.LIGHTYELLOW_EX + "\nThe file is done processing!" + Style.RESET_ALL)


# --------------------------------INSTRUCTION---------------------------------------------------------------


""" 
Điền list poinlogID mà cần crawl lại ở dưới đây nhé
Example:
reset_crawlingtaskid = ['DDFCACEA9584463282EBD00150337A8A',
'F22F3FCC48D847A0B7A7613F8B7D1AF8']
"""

reset_crawlingtaskid = []


# ------------------------------------END-------------------------------------------------------------------

reset_crawlings(reset_crawlingtaskid)
