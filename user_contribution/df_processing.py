import pandas as pd
# from notItunes_album import find_dfcolumn, find_rowdf

def find_rowdf(df, column_name):
    header_row = str(df.loc[(df == column_name).any(axis=1)].index)
    for i in ["Int64Index([", "], dtype='int64')"]:
        header_row = header_row.replace(i, "")
    # print(type(header_row))
    # print(header_row)
    header_row = int(header_row)
    # print(header_row)
    return header_row


def find_dfcolumn(df, column_name):
    header_row = find_rowdf(df, column_name)
    df.index += 1
    new_header = df.iloc[header_row]  # create column title
    df.columns = new_header
    df_tocheck_ori = df[header_row + 1 :]
    df_tocheck_ori = df_tocheck_ori.loc[:, ~df_tocheck_ori.columns.duplicated()]
    return df_tocheck_ori

class df_processing:
    def __init__(self, contribution, open_urls):
        self.contribution = contribution
        self.url = open_urls
    
    def get_filename(self):
        file_name = self.url.title
        return file_name

    def create_sheet(self):
        sheet = self.url.worksheet(self.contribution.sheetname)
        return sheet

    def create_df_ori(self):
        sheet = self.create_sheet()
        data = sheet.get_all_values()
        df_ori = pd.DataFrame(data)
        return df_ori

    def create_df_tocheck_ori(self):
        # Tạo df với columns có chứa PointlogsID
        df_ori = self.create_df_ori()
        df_tocheck_ori = find_dfcolumn(df_ori, "PointlogsID")
        # df_tocheck_ori = df_tocheck_ori.loc[:, ~df_tocheck_ori.columns.duplicated()]
        return df_tocheck_ori

    def create_df_tocheck_ori_trackid(self):
        # Tạo df với columns có chứa PointlogsID
        df_ori = self.create_df_ori()
        df_tocheck_ori = find_dfcolumn(df_ori, "track_id")
        # df_tocheck_ori = df_tocheck_ori.loc[:, ~df_tocheck_ori.columns.duplicated()]
        return df_tocheck_ori

    def create_prevalid_row(self):
        # Xác định dòng cuối giá trị prevalid và tạo df với các dòng sau prevalid
        df_ori = self.create_df_ori()
        find_dfcolumn(df_ori, "PointlogsID")
        prevalid_row = find_rowdf(df_ori, "pre_valid")
        return prevalid_row

    def create_last_prevalid_row_ori(self):
        df_tocheck_ori = self.create_df_tocheck_ori()
        df_prevalid = df_tocheck_ori["pre_valid"][df_tocheck_ori["pre_valid"] != ""]
        # print(df_prevalid)
        if df_prevalid.empty:
            last_prevalid_row_ori = self.create_prevalid_row()
        else:
            last_prevalid_row_ori = df_prevalid[-1:].index.tolist()[0]

        return last_prevalid_row_ori

    def create_df_tocheck(self):
        last_prevalid_row_ori = self.create_last_prevalid_row_ori()
        prevalid_row = self.create_prevalid_row()
        df_tocheck_ori = self.create_df_tocheck_ori()
        last_prevalid_row = last_prevalid_row_ori - prevalid_row
        df_tocheck = df_tocheck_ori[last_prevalid_row:]
        return df_tocheck

    def create_df_approve(self):
        df_tocheck = self.create_df_tocheck()
        df_approve = df_tocheck[df_tocheck[self.contribution.column_filter] != "not found"]
        # df_approve = create_df_approve(df_tocheck, self.contribution)
        return df_approve
    
    def create_df_reject(self):
        df_tocheck = self.create_df_tocheck()
        df_reject = df_tocheck[df_tocheck[self.contribution.column_filter] == "not found"]
        # df_approve = create_df_approve(df_tocheck, self.contribution)
        return df_reject