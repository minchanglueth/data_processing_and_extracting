from contribute_artist_or_album import check_valid_empty_dup, check_image_and_album_status, Contributions, extract_MA_MAA_remove_dup
from notItunes_album import run_albumnotitunes, MAA_Contribution_notItunes
from notItunes_recheckpl import extract_report, extract_similarity, check_and_update, CY_Contribution_notItunes
from connect_confidential.connect_gspread import client_gspread

url = "https://docs.google.com/spreadsheets/d/1ZUzx1smeyIKD4PtQ-hhT1kbPSTGRdu8I8NG1uvzcWr4/edit#gid=574026380" # thay đổi link contribution ở đây
open_urls = client_gspread.open_by_url(url)
pre_valid_list = ['2021-08-13'] # input valid_date vào đây

option = input('Please select options: missing_artist, missing_album, notItunes_album, notItunes_recheckpl, extract_ma_maa_mp3_mp4 ')

if option == 'missing_artist':
    action = input('Please select actions: update_pointlogs, check_crawling_status ')
    if action == 'update_pointlogs':
        check_valid_empty_dup(Contributions.MA_Contribution, open_urls)
    elif action == 'check_crawling_status':
        answer = input('YES if finish update pre_valid_list ? ')
        if answer == 'YES':
            check_image_and_album_status(Contributions.MA_Contribution, pre_valid_list, open_urls)
elif option == 'missing_album':
    action = input('Please select actions: update_pointlogs, check_crawling_status ')
    if action == 'update_pointlogs':
        check_valid_empty_dup(Contributions.MAA_Contribution, open_urls)
    elif action == 'check_crawling_status':
        answer = input('YES if finish update pre_valid_list ? ')
        if answer == 'YES':
            check_image_and_album_status(Contributions.MAA_Contribution, pre_valid_list, open_urls)
elif option == 'notItunes_album':
    action = input('Please select actions: check_and_update, check_crawling_status ')
    if action == 'check_and_update':
        run_albumnotitunes(open_urls)
    elif action == 'check_crawling_status':
        check_image_and_album_status(MAA_Contribution_notItunes, None, open_urls)
elif option == 'notItunes_recheckpl':
    action = input('Please select actions: extract_report, extract_similarity, check_and_update, check_crawling_status ')
    if action == 'extract_report':
        extract_report(open_urls)
    elif action == 'extract_similarity':
        extract_similarity(open_urls)
    elif action == 'check_and_update':
        check_and_update(open_urls)
    elif action == 'check_crawling_status':
        answer = input('YES if finish update pre_valid_list ? ')
        if answer == 'YES':
            check_image_and_album_status(CY_Contribution_notItunes,pre_valid_list, open_urls)
elif option == 'extract_ma_maa_mp3_mp4':
    extract_MA_MAA_remove_dup(open_urls, Contributions.MA_MAA_MP_3)
    extract_MA_MAA_remove_dup(open_urls, Contributions.MA_MAA_MP_4)
