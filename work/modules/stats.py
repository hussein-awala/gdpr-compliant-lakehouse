from os import listdir
from os.path import join, isdir, isfile


def get_users_export_stats(ds_path):
    max_files_by_user = 0
    min_files_by_user = 1000000
    nb_files = 0
    nb_users = 0

    for user_folder in listdir(ds_path):
        user_folder_path = join(ds_path, user_folder)
        if not isdir(user_folder_path):
            # not a user folder
            continue
        nb_files_for_user = 0
        for file in listdir(user_folder_path):
            file_path = join(user_folder_path, file)
            if not isfile(file_path) or not file_path.endswith(".parquet"):
                # not a parquet file
                continue
            nb_files_for_user += 1
        max_files_by_user = max(max_files_by_user, nb_files_for_user)
        min_files_by_user = min(min_files_by_user, nb_files_for_user)
        nb_files += nb_files_for_user
        nb_users += 1
    return {
        "max_files_by_user": max_files_by_user,
        "min_files_by_user": min_files_by_user,
        "avg_files_by_user": nb_files / nb_users,
    }
