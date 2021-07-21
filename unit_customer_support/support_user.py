from connect_confidential.connect_db_final import (
    ssh_host,
    ssh_port,
    ssh_user,
    mypkey,
    conn,
    cursor,
)

import paramiko

from colorama import Fore, Style
import pandas as pd
from datetime import date

from slack_report import send_message_slack
from update_data_report import update_data_gsheet


def ssh_command(selected_option, command, users_column, input_user, user_ori_info):
    s = paramiko.SSHClient()
    s.load_system_host_keys()
    s.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    s.connect(hostname=ssh_host, port=ssh_port, username=ssh_user, pkey=mypkey)
    (stdin, stdout, stderr) = s.exec_command(command)
    for line in stdout.readlines():
        status = selected_option + " status: " + line
        send_message_slack(user_ori_info, status).msg_slack()
        send_message_slack(user_ori_info, status).send_to_slack()
    s.close()


def delete_command(userid):
    delete_command = (
        """curl --request DELETE --url 'http://ec2-beta-api-v8.vibbidi.local:8045/users/"""
        + str(userid)
        + """?mpk=e429d5baeb58a5608e28c5f241e3e500e74f064b2&muid=896288923398116&muname=photon'"""
    )
    return delete_command


def changepass_command(username, new_password):
    command = "curl --request POST\
 --url 'http://ec2-beta-api-v8.vibbidi.local:8045/users/setNewPassword?mpk=e429d5baeb58a5608e28c5f241e3e500e74f064b2&muid=498273542225616&muname=kaz_piro_piro'\
 --form new_password={}\
 --form username={}"
    pass_command = command.format(new_password, username)
    return pass_command


check_username = """select id, Username, Email from v4.users where {} = '{}'"""
update_username = """UPDATE v4.users set Username = '{}' where Username = '{}'"""


def df_to_value(df, column_name):
    value = str(df[column_name].astype(str).values).strip("[]").strip("'")
    return value


def check_user(query, users_column, input_user, for_update_username):
    query = query.format(users_column, input_user)
    cursor.execute(query)
    conn.commit()
    if for_update_username != "yes":
        result = cursor.fetchall()
        sql1 = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
        user_info = (
            df_to_value(sql1, "id")
            + " / "
            + df_to_value(sql1, "Username")
            + " / "
            + df_to_value(sql1, "Email")
        )
        return user_info


def split(query, users_column, input_user, order):
    user_info = check_user(query, users_column, input_user, "no")
    list = user_info.split("/")
    info = list[order].strip()
    return info


def check_update(
    selected_option,
    users_column,
    input_user,
    command,
    old_username,
    userid,
    old_email,
    new_username,
):
    print(
        Fore.LIGHTMAGENTA_EX + "\nPlease Recheck user info as below:" + Style.RESET_ALL
    )
    print("\rid / username / email: ", end="")
    print(check_user(check_username, users_column, input_user, "no"))
    if split(check_username, users_column, input_user, 0) != "":
        user_ori_info = check_user(check_username, users_column, input_user, "no")
        choose = input(
            Fore.LIGHTCYAN_EX
            + "\nEnter YES for "
            + selected_option
            + ", NO to exit: "
            + Style.RESET_ALL
        )
        if choose == "YES":
            ssh_command(
                selected_option, command, users_column, input_user, user_ori_info
            )
            update_data_gsheet(
                str(date.today()),
                selected_option,
                userid,
                old_email,
                old_username,
                new_username,
            )
        else:
            print("\n" + Fore.LIGHTRED_EX + "NO CHANGES MADE!" + Style.RESET_ALL)
    else:
        print(Fore.LIGHTRED_EX + "No such user exists !" + Style.RESET_ALL)


def user_support():
    print(
        "Please choose one option: user_change_password, user_change_username, user_delete_account"
    )
    selected_option = input(
        Fore.LIGHTCYAN_EX + "\nPlease input the option: " + Style.RESET_ALL
    )
    if selected_option == "user_change_password":
        username = input(
            Fore.LIGHTCYAN_EX + "\nPlease input username: " + Style.RESET_ALL
        )
        new_password = input(
            Fore.LIGHTCYAN_EX + "\nPlease input new password: " + Style.RESET_ALL
        )
        check_update(
            selected_option,
            "Username",
            username,
            changepass_command(username, new_password),
            username,
            split(check_username, "Username", username, 0),
            split(check_username, "Username", username, 2),
            None,
        )
    elif selected_option == "user_delete_account":
        userid = input(Fore.LIGHTCYAN_EX + "\nPlease input userid: " + Style.RESET_ALL)
        check_update(
            selected_option,
            "id",
            userid,
            delete_command(userid),
            split(check_username, "id", userid, 1),
            userid,
            split(check_username, "id", userid, 2),
            None,
        )
    elif selected_option == "user_change_username":
        old_username = input(
            Fore.LIGHTCYAN_EX + "\nPlease input old_username: " + Style.RESET_ALL
        )
        new_username = input(
            Fore.LIGHTCYAN_EX + "\nPlease input new_username: " + Style.RESET_ALL
        )
        print(
            Fore.LIGHTMAGENTA_EX
            + "\nPlease Recheck user info as below:"
            + Style.RESET_ALL
        )
        print(
            "\rid / username / email: ",
            check_user(check_username, "Username", old_username, "no"),
        )
        if split(check_username, "Username", old_username, 0) != "":
            choose = input(
                Fore.LIGHTCYAN_EX
                + "\nEnter YES to update username, NO to exit: "
                + Style.RESET_ALL
            )
            if choose == "YES":
                check_user(update_username, new_username, old_username, "yes")
                print(
                    "\rUpdated id / username / email: ",
                    check_user(check_username, "Username", new_username, "no"),
                )
                added_noti = (
                    "old username: " + old_username + "\r+ user_change_username: done"
                )
                send_message_slack(
                    check_user(check_username, "Username", new_username, "no"),
                    added_noti,
                ).send_to_slack()
                update_data_gsheet(
                    str(date.today()),
                    selected_option,
                    split(check_username, "Username", new_username, 0),
                    split(check_username, "Username", new_username, 2),
                    old_username,
                    new_username,
                )
            else:
                print("\n" + Fore.LIGHTRED_EX + "NO CHANGES MADE!" + Style.RESET_ALL)
        else:
            print(Fore.LIGHTRED_EX + "No such user exists !" + Style.RESET_ALL)
    else:
        print(
            "\n"
            + Fore.LIGHTRED_EX
            + "The input is WRONG , please RE-ENTER later!"
            + Style.RESET_ALL
        )

    print("\n" + Fore.LIGHTYELLOW_EX + "The file is done processing!" + Style.RESET_ALL)
