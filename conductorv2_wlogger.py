#region imports
import smartsheet
from smartsheet_grid import grid
import time
from datetime import datetime
import pandas as pd
from globals import sensative_smartsheet_token
from logger import ghetto_logger
#endregion

class ConductorV2:
    def __init__(self, config):
        self.config=config
        self.smart = smartsheet.Smartsheet(self.config.get("stoken"))
        self.smart.errors_as_exceptions(True)
        grid.token=self.config.get("stoken")
        self.conductor_sheet_id = config.get("conductor_sheet_id")
        self.log=ghetto_logger("conductorv2_wlogger.py")
        self.sheet_id_to_full_dict = lambda sheet_id: self.smart.Sheets.get_columns(sheet_id,level=2).to_dict()
        self.conductor_sheet_df, self.conductor = self.fetch_df(self.conductor_sheet_id)
        self.start_time = time.time()
        self.gather_column_ids()
    # region data gather/preprocessing
    def fetch_df(self, sheet_id):
        '''fetches data from smartsheet, and returns row data'''
        df=grid(sheet_id)
        df.fetch_content()
        df_obj = df.df
        return df, df_obj
    def gather_column_ids(self):
        '''gathers column ids from current conductor sheet id (incase we want to switch out the conductor sheet)'''
        cdf = self.conductor_sheet_df.column_df
        try:
            self.columnid_ROW_ID = cdf.loc[cdf['title'] == "ROW_ID"]['id'].tolist()[0]
            self.columnid_CONDUCTOR_rowid = cdf.loc[cdf['title'] == "CONDUCTOR_rowid"]['id'].tolist()[0]
            self.columnid_ENABLED = cdf.loc[cdf['title'] == "ENABLED"]['id'].tolist()[0]
            self.columnid_DESCRIPTION = cdf.loc[cdf['title'] == "DESCRIPTION"]['id'].tolist()[0]
            self.columnid_WEBHOOK_ID = cdf.loc[cdf['title'] == "WEBHOOK_ID"]['id'].tolist()[0]
            self.columnid_SOURCE_sheet_name = cdf.loc[cdf['title'] == "SOURCE_sheet_name"]['id'].tolist()[0]
            self.columnid_SOURCE_sheet_id = cdf.loc[cdf['title'] == "SOURCE_sheet_id"]['id'].tolist()[0]
            self.columnid_SOURCE_column_name = cdf.loc[cdf['title'] == "SOURCE_column_name"]['id'].tolist()[0]
            self.columnid_SOURCE_column_id = cdf.loc[cdf['title'] == "SOURCE_column_id"]['id'].tolist()[0]
            self.columnid_DESTINATION_sheet_id = cdf.loc[cdf['title'] == "DESTINATION_sheet_id"]['id'].tolist()[0]
            self.columnid_DESTINATION_column_name = cdf.loc[cdf['title'] == "DESTINATION_column_name"]['id'].tolist()[0]
            self.columnid_DESTINATION_column_id = cdf.loc[cdf['title'] == "DESTINATION_column_id"]['id'].tolist()[0]
            self.columnid_DESTINATION_dropdown_type = cdf.loc[cdf['title'] == "DESTINATION_dropdown_type"]['id'].tolist()[0]
            self.columnid_PYTHON_MESSAGE = cdf.loc[cdf['title'] == "PYTHON_MESSAGE"]['id'].tolist()[0]
        except IndexError:
            self.log.log("failed to find column_ids, check that the column names have not changed and try again")
    def fetch_sheet_grid_obj(self, sheet_id):
        df=grid(sheet_id)
        df.fetch_content()
        return df
    def timestamp(self): 
        '''creates a string of minute/second from start_time until now for logging'''
        end_time = time.time()  # get the end time of the program
        elapsed_time = end_time - self.start_time  # calculate the elapsed time in seconds       

        minutes, seconds = divmod(elapsed_time, 60)  # convert to minutes and seconds       
        timestamp = "{:02d}:{:02d}".format(int(minutes), int(seconds))
        
        return timestamp
    def ss_log(self, row_id, message_string, with_print=True):
        '''logs error/success message to log column in Conductor sheet on Smartsheet'''
        new_cell = self.smart.models.Cell()
        new_cell.column_id = self.columnid_PYTHON_MESSAGE
        new_cell.value = message_string
        new_cell.strict = False
        
        new_row = self.smart.models.Row()
        new_row.id =  int(row_id)
        new_row.cells.append(new_cell)

        self.smart.Sheets.update_rows(self.conductor_sheet_id, [new_row])
        if with_print:
            self.log.log(f"Logged: {message_string}")
    def ss_post(self, column_id, column_name, row_id, post_value, with_log=True, with_print=False):
        '''same as log, but lets you post to any column. This function exists for posting data, where log exists for logging messages from code'''
        new_cell = self.smart.models.Cell()
        new_cell.column_id = column_id
        new_cell.value = post_value
        new_cell.strict = False
        
        new_row = self.smart.models.Row()
        new_row.id =  int(row_id)
        new_row.cells.append(new_cell)

        self.smart.Sheets.update_rows(self.conductor_sheet_id, [new_row])
        message = f"Posted to {column_name}: {post_value}"
        if with_print:
            self.log.log(message)
        if with_log:
            self.ss_log(row_id, message, with_print)
    def filterin_focused_rows(self, row_list, input_rowid_list):
        '''shortens the row list to just rows that have a row id matching the rowid list used to run a "focused run" and only run certain rows'''
        focused_row_list = []
        for row in row_list:
            for id in input_rowid_list:
                if str(row.get("CONDUCTOR_rowid"))==str(id):
                    focused_row_list.append(row)
        return focused_row_list
    def source_column_id(self, row_index):
        '''either returns a column_id if it's already there, or returns None'''
        if self.conductor['SOURCE_column_id'][row_index] != "":
            SOURCE_column_id = self.conductor['SOURCE_column_id'][row_index] 
        else:
            SOURCE_column_id = None
        return SOURCE_column_id
    def destination_column_id(self, row_index):
        '''either returns a column_id if it's already there, or returns None'''
        if self.conductor['DESTINATION_column_id'][row_index] != "":
            DESTINATION_column_id = self.conductor['DESTINATION_column_id'][row_index] 
        else:
            DESTINATION_column_id = None
        return DESTINATION_column_id
    def conductor_row_id(self, row_index):
        '''checks to see if the row id is posted, and if not posts it'''
        if self.conductor['CONDUCTOR_rowid'][row_index] == None:
            conductor_rowid = str(self.conductor['id'][row_index])
            self.ss_post(self.columnid_CONDUCTOR_rowid, "CONDUCTOR_rowid", conductor_rowid, conductor_rowid)
        else:
            conductor_rowid = self.conductor['CONDUCTOR_rowid'][row_index]
        return conductor_rowid
    def generate_conductor_dict(self):
        '''makes dict object with all data needed for rest of script'''
        self.log.log(f'{self.timestamp()} gathering data...')
        row_objects = []
        for row_index in range(len(self.conductor['id'].tolist())):
            if self.conductor['SOURCE_sheet_id'][row_index] != None and not("header" in self.conductor['SOURCE_sheet_id'][row_index]):
            # if the sheet_id is blank, it will not work anyways (and we are likely working with an enabled row or roll up header)
                row_dict ={
                    'CONDUCTOR_rowid' : self.conductor_row_id(row_index),
                    'ROW_ID' : self.conductor['ROW_ID'][row_index],
                    'ENABLED': self.conductor['ENABLED'][row_index],
                    'SOURCE_sheet_name':self.conductor['SOURCE_sheet_name'][row_index],
                    'SOURCE_sheet_id': self.conductor['SOURCE_sheet_id'][row_index],
                    'SOURCE_column_name':self.conductor['SOURCE_column_name'][row_index],
                    'SOURCE_column_id': self.source_column_id(row_index),
                    'DESTINATION_sheet_id':self.conductor['DESTINATION_sheet_id'][row_index],
                    'DESTINATION_column_name':self.conductor['DESTINATION_column_name'][row_index],
                    'DESTINATION_column_id':self.destination_column_id(row_index),
                    'DESTINATION_dropdown_type':self.conductor['DESTINATION_dropdown_type'][row_index],
                    'index':row_index
                    }
                if row_dict not in row_objects:
                    row_objects.append(row_dict)

        return row_objects
    # endregion
    # region auditing (data = self.row_list)
    def auditdata_transformation(self, row_data):
        '''reorganizes the data by sheet id to improve code's efficancy. This way, it only uses the api (to pull sheet data) once per sheet in use'''
        source_sheet_id_list = []
        source_audit_list = []

        destination_sheet_id_list = []
        destination_audit_list = []
        for row in row_data:
            # source
            if row.get('SOURCE_sheet_id') not in source_sheet_id_list:
                source_sheet_id_list.append(row.get('SOURCE_sheet_id'))
                source_audit_list.append({row.get('SOURCE_sheet_id'):[row]})
            else:
                for sheet_id in source_audit_list:
                    if list(sheet_id.keys())[0] == row.get('SOURCE_sheet_id'):
                        sheet_id[row.get('SOURCE_sheet_id')].append(row)
            
            #destination
            if row.get('DESTINATION_sheet_id') not in destination_sheet_id_list:
                destination_sheet_id_list.append(row.get('DESTINATION_sheet_id'))
                destination_audit_list.append({row.get('DESTINATION_sheet_id'):[row]})
            else:
                for sheet_id in destination_audit_list:
                    if list(sheet_id.keys())[0] == row.get('DESTINATION_sheet_id'):
                        sheet_id[row.get('DESTINATION_sheet_id')].append(row)
        return source_audit_list, destination_audit_list
    def ssdata_audit(self, row_list, location_str):
        '''audits the raw row_data by looping through it looking for possible errors (missing source id, wrong column types, etc...)
        location str specificies SOURCE or DESTINATION'''
        self.log.log(f'{self.timestamp()} auditing {location_str.lower()} data...')
        for row_cluster in row_list:
                try:
                    sheet_id = list(row_cluster.keys())[0]
                    df, sheet = self.audit_sheet_id(sheet_id, row_cluster, location_str)
                except TypeError:
                    self.log.log("cluster skipped")
                for row in row_cluster.get(sheet_id):
                    try:
                        row[f'{location_str}_grid_obj']=sheet
                        if row.get(f'{location_str}_column_id') == None:
                            self.fetch_columnid_w_columname(row, df, location_str)
                        self.audit_columntitle_against_columnid(row, df, location_str)
                        self.find_column_index(row, df, location_str)
                    except:
                        self.log.log('row skipped')
    def audit_sheet_id(self, sheet_id, row_cluster, location_str):
        '''tries to fetch the data for the sheet, and if it fails, logs the error message'''
        try:
            sheet = self.fetch_sheet_grid_obj(sheet_id)
            df = sheet.column_df
            return df, sheet
        except:
            for row in row_cluster.get(sheet_id):
                self.ss_log(row['CONDUCTOR_rowid'], f"{location_str} SHEET ID ERROR: Sheet ID not found, check that the ID is right, and shared w/ automation@dowbuilt.com")
            return 
    def fetch_columnid_w_columname(self, row, df, location_str):
        '''fetches the column id using the column name for a row'''
        if location_str == "SOURCE":
            posting_column_id = self.columnid_SOURCE_column_id
        else:
            posting_column_id = self.columnid_DESTINATION_column_id
        
        try:
            column_data = df[df['title'] == row.get(f'{location_str}_column_name')]
            column_id = column_data['id'].tolist()[0]
            row[f'{location_str}_column_id'] = column_id 
            self.ss_post(posting_column_id, f"{location_str}_column_id", row['CONDUCTOR_rowid'], row[f'{location_str}_column_id'])
        except IndexError:
            self.ss_log(row['CONDUCTOR_rowid'], f"{location_str} COLUMN NAME ERROR: Column Name not found on {location_str} sheet (with given sheet id)")
    def audit_columntitle_against_columnid(self, row, df, location_str):
        '''looks at the Source Column on its native sheet, and checks the name of the column that corresponds with the column_id on the row. If there is a discrepency the column_name changes'''
        if location_str == "SOURCE":
            posting_column_id = self.columnid_SOURCE_column_name
        else:
            posting_column_id = self.columnid_DESTINATION_column_name
        
        try:
            column_data = df[df['id'] == int(row.get(f'{location_str}_column_id'))]
            column_name = column_data['title'].tolist()[0]
            if column_name != row[f'{location_str}_column_name']:
                row[f'{location_str}_column_name'] = column_name
                self.ss_post(posting_column_id, f"{location_str}_column_name", row['CONDUCTOR_rowid'], row[f'{location_str}_column_name'])
        except IndexError:
            self.ss_log(row['CONDUCTOR_rowid'], f"{location_str} COLUMN ID ERROR: Column ID not found on {location_str} sheet (with given sheet id)")
            self.fetch_columnid_w_columname(row, df, location_str)
    def find_column_index(self, row, df, location_str):
        '''used to update the row when data is large and not in df, data can be located purely by index'''
        for count, column_name in enumerate(df['title'].tolist()):
            if column_name == row.get(f'{location_str}_column_name'):
                row[f'{location_str}_column_index'] = count
    #endregion
    # region posting dropdowns (data = self.inputs)
        # region value bundling 
    def extract_column_info_dict(self):
        get_desired_column_data = lambda row: row.get("cells")[self.inputs.get('SOURCE_column_index')] 
        thorough_sheet_obj = self.smart.Sheets.get_sheet(self.inputs.get("SOURCE_sheet_id"), include="objectValue", level=2).to_dict().get("rows")
        self.column_content_dict = [get_desired_column_data(row) 
            for row in 
            thorough_sheet_obj]
    def contact_r_multi_data(self, objectValue):
            try: 
                #for single contact
                return [item["objectValue"][objectValue] for item in self.column_content_dict if "objectValue" in item]
            except:
                #for first item of multi contact
                return [item["objectValue"]["values"][0][objectValue] for item in self.column_content_dict if "objectValue" in item]   
    def extract_name_n_email_list(self):
        try:
            column_content_object = pd.DataFrame({
                "email":self.contact_r_multi_data("email"),
                "name":self.contact_r_multi_data("name")})
            self.contact_list = column_content_object.drop_duplicates().to_dict("records")
        except:
            if self.log and self.error_message == False:
                self.self.log.log(self.row_id, "EXTRACTING EMAILS ERROR: Check that your source is a CONTACT column, and that each value is a CONTACT, then check that the desired column in your destination is CONTACT column")
                self.error_message = True
            self.contact_list=[]
    def clean_contact_list(self):
        '''this is where we take the list and just only use non-blank, non-duplictes'''
        value_bundle=[]
        return [contact for contact in self.contact_list if not contact in value_bundle]
    def clean_pick_list(self):
        '''grabs all values from columns that are not blank and not duplicates. This is for pick lists only'''
        value_bundle = []
        for item in self.inputs.get('SOURCE_grid_obj').df[self.inputs.get('SOURCE_column_name')].tolist():
            if item != None:
                if not(item in value_bundle):
                    value_bundle.append(item)
        return value_bundle
    def gather_dropdown_values(self):
        '''gather/clean data that will become the dropdown options in the destinations based on type'''
        if self.inputs.get("DESTINATION_dropdown_type") in ['picklist','multi-picklist']:
            value_bundle = self.clean_pick_list()
        elif self.inputs.get("DESTINATION_dropdown_type") in ['contact', 'multi-contact']:
            self.extract_column_info_dict()
            self.extract_name_n_email_list()
            value_bundle = self.clean_contact_list()
        else:
            self.ss_log(self.inputs.get('CONDUCTOR_rowid'), "DESTINATION_dropdown_type ERROR: the Dropdown type did not match the required options")
        self.inputs['value_bundle'] = value_bundle
        # endregion
        # region value posting 
    def picklist_updater(self):
        '''posts if the type is picklist'''
        self.inputs['column_update'] = self.smart.models.Column({
            'type': 'PICKLIST',
            'options' : self.inputs.get("value_bundle"),
            'validation' : False,
            'overrideValidation' : True
        })
    def multipicklist_updater(self):
        '''posts if the type is multi picklist'''
        self.inputs['column_update'] = self.smart.models.Column({
        'type': 'MULTI_PICKLIST',
        'options' :  self.inputs.get("value_bundle"),
        'validation' : False,
        'overrideValidation' : True
        })
    def contact_updater(self):
        '''posts if the type is contact'''
        self.inputs['column_update'] = self.smart.models.Column({
            'override_validation' : 'True',
            'formula' : "",
            'type' : "CONTACT",
            'contactOptions' : 
                self.inputs.get("value_bundle"),
                'type' : 'CONTACT_LIST'
        })
    def multicontact_updater(self):
        '''posts if hte type is multi contact'''
        self.inputs['column_update']= self.smart.models.Column({
            'override_validation' : 'True',
            'formula' : "",
            'type' : "MULTI_CONTACT_LIST",
            'contactOptions' : 
                self.inputs.get("value_bundle"),
                'type' : 'MULTI_CONTACT_LIST'
        })
    def post_update(self, sheet_id, column_id):
        # try:
        self.response = self.smart.Sheets.update_column(
        sheet_id, 
        column_id, 
        self.inputs['column_update']
        )
        # except:
        #     self.ss_log(self.inputs.get("CONDUCTOR_rowid"), "Post_Update failed!")
    def dynamic_column_update(self, sheet_id, column_id):
        '''checks the column type, and chooses the correct posting function to match the column type'''
        if self.inputs.get("DESTINATION_dropdown_type") == "picklist":
            self.picklist_updater()
        elif self.inputs.get("DESTINATION_dropdown_type") == "multi-picklist":
            self.multipicklist_updater()
        elif self.inputs.get("DESTINATION_dropdown_type") == "contact":
            self.contact_updater()
        elif self.inputs.get("DESTINATION_dropdown_type") == "multi-contact":
            self.multicontact_updater()
        else:
            self.ss_log(self.inputs.get("CONDUCTOR_rowid"), "dyanmic_column_update failed, Check that the column type has no typos.")
        self.post_update(sheet_id, column_id)  
    def log_successful_post(self):
        '''generates a posting message that says the time/date'''
        now = datetime.now()
        dt_string = now.strftime("%m/%d %H:%M")
        self.ss_log(self.inputs.get("CONDUCTOR_rowid"), f"{dt_string} POSTED")
        # endregion
    def update_columns_dynamic_dropdowns(self, row_data):
        '''same row_data = {'CONDUCTOR_rowid': '364965002733444',
                            'ENABLED': True,
                            'SOURCE_sheet_name': 'Sheet',
                            'SOURCE_sheet_id': '5509709919741828',
                            'SOURCE_column_name': 'TEST',
                            'SOURCE_column_id': '1520064932931460',
                            'DESTINATION_sheet_column_pairs': '5509709919741828:~TEST~',
                            'DESTINATION_dropdown_type': 'picklist',
                            'index': 0,
                            'source_grid_obj': <smartsheet_grid.smartsheet_grid.grid at 0x2bf80ef6350>,
                            'column_index':3}'''
        self.error_message = False
        self.inputs = row_data
        self.gather_dropdown_values()
        self.dynamic_column_update(self.inputs.get("DESTINATION_sheet_id"), self.inputs.get("DESTINATION_column_id"))
        self.log_successful_post()
    #endregion
    # region run configurations
    def run_dynamic_dropdowns(self, row_list):
        '''loops through each item in list that is enabled, and executes that column update (to refresh the dynamic dropdown options in that column)'''
        self.log.log(f'{self.timestamp()} initiating column updates...')
        for count, row_data in enumerate(row_list):
            try:
                self.log.log(f'{self.timestamp()} updating row {row_data.get("ROW_ID")} ({count+1} of {str(len(row_list))})')
                self.update_columns_dynamic_dropdowns(row_data)
            except:
                self.ss_log(row_data.get("CONDUCTOR_rowid"), "Post_Update failed!")
    def focused_run(self, input_rowid_list):
        '''executes the dynamic dropdown update on specific row id(s) in input list i.e. [364965002733444]'''
        self.row_list = self.generate_conductor_dict()
        self.focused_row_list = self.filterin_focused_rows(self.row_list, input_rowid_list)
        self.source_audit, self.destination_audit = self.auditdata_transformation(self.focused_row_list)
        self.ssdata_audit(self.source_audit, "SOURCE")
        self.ssdata_audit(self.destination_audit, "DESTINATION")
        self.run_dynamic_dropdowns(self.focused_row_list)
        self.log.log(f'{self.timestamp()} fin')
    def cron_run(self):
        '''executes the dynamic dropdown update on all rowids in the conductor sheet (that are enabled)'''
        self.row_list = self.generate_conductor_dict()
        self.source_audit, self.destination_audit = self.auditdata_transformation(self.row_list)
        self.ssdata_audit(self.source_audit, "SOURCE")
        self.ssdata_audit(self.destination_audit, "DESTINATION")
        self.run_dynamic_dropdowns(self.row_list)
        self.log.log(f'{self.timestamp()} fin')

    # endregion

if __name__ == "__main__":
    config = {'stoken':sensative_smartsheet_token, 'conductor_sheet_id': 7237912061339524}
    con = ConductorV2(config)
    con.cron_run()
    # con.focused_run([5103066726305668])