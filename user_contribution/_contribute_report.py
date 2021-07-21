from contribute_artist_or_album import check_valid_empty_dup, check_image_and_album_status, Contributions
from notItunes_album import run_albumnotitunes
from notItunes_recheckpl import extract_report, extract_similarity, check_and_update

pre_valid_list = ['2021-07-12']

option = input('Please select options: missing_artist, missing_album, notItunes_album, notItunes_recheckpl ')

if option == 'missing_artist':
    action = input('Please select actions: update_pointlogs, check_crawling_status ')
    if action == 'update_pointlogs':
        check_valid_empty_dup(Contributions.MA_Contribution)
    elif action == 'check_crawling_status':
        answer = input('YES if finish update pre_valid_list ? ')
        if answer == 'YES':
            check_image_and_album_status(Contributions.MA_Contribution, pre_valid_list)
elif option == 'missing_album':
    action = input('Please select actions: update_pointlogs, check_crawling_status ')
    if action == 'update_pointlogs':
        check_valid_empty_dup(Contributions.MAA_Contribution)
    elif action == 'check_crawling_status':
        answer = input('YES if finish update pre_valid_list ? ')
        if answer == 'YES':
            check_image_and_album_status(Contributions.MAA_Contribution,pre_valid_list)
elif option == 'notItunes_album':
    run_albumnotitunes()
elif option == 'notItunes_recheckpl':
    action = input('Please select actions: extract_report, extract_similarity, check_and_update ')
    if action == 'extract_report':
        extract_report()
    elif action == 'extract_similarity':
        extract_similarity()
    elif action == 'check_and_update':
        check_and_update()