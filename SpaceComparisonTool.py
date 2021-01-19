from zeep import Client
import concurrent.futures
import time
import pandas as pd
import logging
import os
import sys

# Variables Declaration
space1ID = ""
space2ID = ""
folders_list1 = []
folders_list2 = []
folders_last_modified1 = []
folders_last_modified2 = []
directory_permissions1 = []
directory_permissions2 = []
DataSources_Code = "1"
Hierarchies_code = "2"
Varaibles_code = "3"
FolderPermissions_code = "4"
SubjectAreaPermissions_code = "5"
Catalog_code = "6"
config_file = 'config.txt'
log_file = 'logfile.txt'
print("Process Started.......")

try:
    with open(config_file) as f:
        for line in f:
            if line.startswith('URL'):
                url = line.partition('=')[2].strip()
            elif line.startswith('username'):
                username = line.partition('=')[2].strip()
            elif line.startswith('password'):
                password = line.partition('=')[2].strip()
            elif line.startswith('space1'):
                space1 = line.partition('=')[2].strip()
            elif line.startswith('space2'):
                space2 = line.partition('=')[2].strip()
            elif line.startswith('directory'):
                directory = line.partition('=')[2].strip()
            elif line.startswith('options'):
                options_list = line.partition('=')[2].strip().split(',')
                options_list = [x.strip() for x in options_list]

    # Setting logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    formatter1 = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    formatter2 = logging.Formatter('%(asctime)s:%(levelname)s:%(lineno)d:%(message)s',
                                   datefmt='%m/%d/%Y %I:%M:%S %p')
    filehandler = logging.FileHandler(log_file, mode='w')
    console = logging.StreamHandler()
    filehandler.setFormatter(formatter2)
    filehandler.setLevel(logging.DEBUG)
    console.setFormatter(formatter1)
    console.setLevel(logging.INFO)
    logger.addHandler(filehandler)
    logger.addHandler(console)

    if not url:
        exit_code = 2
        raise RuntimeError('Birst login URL not given..')

    if not username:
        exit_code = 2
        raise RuntimeError('Birst username is not given..')

    if not password:
        exit_code = 2
        raise RuntimeError('Birst password is not given..')

    if not space1:
        exit_code = 2
        raise RuntimeError('Birst Space1 is not given..')

    if not space2:
        exit_code = 2
        raise RuntimeError('Birst Space2 is not given..')

    if not options_list[0]:
        options_list = ['1', '2', '3','4','5','6']



except Exception as e:
    logger.error("An unexpected error has occurred initialising input args " + str(sys.exc_info()[0]))
    logger.error(e)
    sys.exit(exit_code)
try:
    wsdl_loc = url + '/' + 'CommandWebService.asmx?wsdl'
    client = Client(wsdl=wsdl_loc)
    workBookName = space2 + ' vs ' + space1 + '.xlsx'
    writer = pd.ExcelWriter(workBookName, engine='xlsxwriter')


    def login(username, password):
        login_token = client.service.Login(username, password)
        return login_token


    login_token = login(username, password)

    if login_token:
        logger.info('Birst Login with ' + username + ' Successful')
    else:
        logger.error('Birst Login with ' + username + ' Failed')


    # List Spaces

    def list_spaces(loginToken):
        spaces_list = client.service.listSpaces(loginToken)
        return spaces_list


    spacesList = list_spaces(login_token)


    # Get SpaceID
    def get_spaceID(spaces):
        global space1ID, space2ID
        for i in range(len(spaces)):
            if spaces[i]['name'] == space1:
                space1ID = spaces[i]['id']
            elif spaces[i]['name'] == space2:
                space2ID = spaces[i]['id']

        return space1ID, space2ID


    logger.info("Getting Spaces Info..")
    space1ID, space2ID = get_spaceID(spacesList)
    if space1ID == '':
        exit_code = 2
        logger.error("Space " + space1 + " Not Found, Exiting..")
        sys.exit(exit_code)
    elif space2ID == '':
        exit_code = 2
        logger.error("Space " + space2 + " Not Found, Exiting..")
        sys.exit(exit_code)
    else:
        logger.info("Spaces Found")


    # Changed Rows Formatting
    def report_diff(x):
        return x[0] if x[0] == x[1] else '{} ---> {}'.format(*x)


    # list Sources
    def get_sources(spaceID):
        sourcesList = client.service.getSourcesList(login_token, spaceID)
        return sourcesList


    # Get Source Details
    def get_source_details(spaceID, sourcename):
        d1 = {}
        column_names = []
        data_types = []
        width = []
        analyzebydate = []
        measure = []
        locktype = []
        enablesecurityfilter = []

        result = client.service.getSourceDetails(login_token, spaceID, sourcename)

        if not result['Disabled']:
            column_props = result['Columns']['SourceColumnSubClass']
            for column in range(len(column_props)):
                try:
                    processing_grp = ','.join(result['SubGroups']['string'])
                except Exception:
                    processing_grp = ''
                column_names.append(column_props[column]['Name'])
                data_types.append(column_props[column]['DataType'])
                enablesecurityfilter.append(column_props[column]['EnableSecutityFilter'])
                width.append(column_props[column]['Width'])
                analyzebydate.append(column_props[column]['AnalyzeByDate'])
                measure.append(column_props[column]['Measure'])
                locktype.append(column_props[column]['LockType'])
            d1['SourceTable'] = sourcename
            d1['SourceColumns'] = column_names
            d1['ProcessingGroup'] = processing_grp
            d1['DataTypes'] = data_types
            d1['Width'] = width
            d1['EnableSecurityFilter'] = enablesecurityfilter
            d1['AnalyzeByDate'] = analyzebydate
            d1['Measure'] = measure
            d1['LockType'] = locktype
            df1 = pd.DataFrame(d1)
            return df1


    if DataSources_Code in options_list:
        data_sources_start_time = time.perf_counter()
        sourcesList1 = get_sources(space1ID)
        sourcesList2 = get_sources(space2ID)
        logger.info("Getting Source Details From " + space1 + " and " + space2)
        logger.info("Waiting.....")
        with concurrent.futures.ThreadPoolExecutor() as executor1:
            source_results1 = [executor1.submit(get_source_details, space1ID, source) for source in sourcesList1]
            source_results2 = [executor1.submit(get_source_details, space2ID, source) for source in sourcesList2]
        logger.info(
            "Getting Source Details Completed.." + " Time Taken: " + str(time.perf_counter() - data_sources_start_time))

        source_space1_df = [f1.result() for f1 in source_results1]
        source_space2_df = [f2.result() for f2 in source_results2]
        logger.info("Comparing Source Columns...")
        # Comparing DataSources
        source_columns1_df = pd.concat(source_space1_df, ignore_index=True)
        source_columns2_df = pd.concat(source_space2_df, ignore_index=True)
        source_columns_list = source_columns2_df.columns.to_list()

        source_columns1_df['Change Type'] = "Old"
        source_columns2_df['Change Type'] = "New"

        # Creating Key Columns
        source_columns1_df['key'] = source_columns1_df['SourceTable'] + "_" + source_columns1_df['SourceColumns']
        source_columns2_df['key'] = source_columns2_df['SourceTable'] + "_" + source_columns2_df['SourceColumns']

        old_rows_source_columns = set(source_columns1_df['key'])
        new_rows_source_columns = set(source_columns2_df['key'])

        dropped_rows_source_columns = (old_rows_source_columns - new_rows_source_columns)
        added_rows_source_columns = (new_rows_source_columns - old_rows_source_columns)

        # Combining Data
        all_data_source_columns = pd.concat([source_columns1_df, source_columns2_df], ignore_index=True)
        all_data_source_columns.drop_duplicates(subset=source_columns_list, keep='last', inplace=True)

        # Getting Duplicates
        dupp_rows_source_columns_list = all_data_source_columns[all_data_source_columns['key'].duplicated() == True][
            'key'].to_list()
        dupes_source_columns_df = all_data_source_columns[
            all_data_source_columns['key'].isin(dupp_rows_source_columns_list)]

        dupes_old_source_columns_df = dupes_source_columns_df[dupes_source_columns_df['Change Type'] == "Old"]
        dupes_new_source_columns_df = dupes_source_columns_df[dupes_source_columns_df['Change Type'] == "New"]
        # dropping version cols
        dupes_old_source_columns_df = dupes_old_source_columns_df.drop(columns=['Change Type'])
        dupes_new_source_columns_df = dupes_new_source_columns_df.drop(columns=['Change Type'])
        # Setting Index to Key
        dupes_old_source_columns_df.set_index('key', inplace=True)
        dupes_new_source_columns_df.set_index('key', inplace=True)
        # Combining both dupes
        df_all_changes_source_columns_df = pd.concat([dupes_old_source_columns_df, dupes_new_source_columns_df],
                                                     axis='columns', keys=['Old', 'New'], join='outer')
        df_all_changes_source_columns_df = df_all_changes_source_columns_df.swaplevel(axis='columns')[
            dupes_new_source_columns_df.columns[0:]]
        df_changed_source_columns_df = df_all_changes_source_columns_df.groupby(level=0, axis=1).apply(
            lambda frame: frame.apply(report_diff, axis=1))
        df_changed_source_columns_df = df_changed_source_columns_df.reset_index()
        df_changed_source_columns_df = df_changed_source_columns_df[source_columns_list]
        df_changed_source_columns_df['Change Type'] = "Changed"

        # Removed Rows
        df_removed_source_columns_df = all_data_source_columns[
            all_data_source_columns['key'].isin(dropped_rows_source_columns)]
        df_removed_source_columns_df = df_removed_source_columns_df.drop(columns=['Change Type', 'key'])
        df_removed_source_columns_df['Change Type'] = 'Removed'

        # Added Rows
        df_added_source_columns_df = all_data_source_columns[
            all_data_source_columns['key'].isin(added_rows_source_columns)]
        df_added_source_columns_df = df_added_source_columns_df.drop(columns=['Change Type', 'key'])
        df_added_source_columns_df['Change Type'] = 'Added'

        # Final DataFrame
        # Writing to Excel
        if not (
                df_changed_source_columns_df.empty or df_added_source_columns_df.empty or df_removed_source_columns_df.empty):
            source_columns_diff_df = pd.concat(
                [df_added_source_columns_df, df_removed_source_columns_df, df_changed_source_columns_df],
                ignore_index=True)
            source_columns_diff_df.to_excel(writer, sheet_name='SourceColumnsdiff', index=False)
        elif not (df_changed_source_columns_df.empty or df_added_source_columns_df.empty):
            source_columns_diff_df = pd.concat(
                [df_added_source_columns_df, df_changed_source_columns_df], ignore_index=True)
            source_columns_diff_df.to_excel(writer, sheet_name='SourceColumnsdiff', index=False)
        elif not (df_changed_source_columns_df.empty or df_removed_source_columns_df.empty):
            source_columns_diff_df = pd.concat(
                [df_changed_source_columns_df, df_removed_source_columns_df], ignore_index=True)
            source_columns_diff_df.to_excel(writer, sheet_name='SourceColumnsdiff', index=False)
        elif not (df_added_source_columns_df.empty or df_removed_source_columns_df.empty):
            source_columns_diff_df = pd.concat(
                [df_added_source_columns_df, df_removed_source_columns_df], ignore_index=True)
            source_columns_diff_df.to_excel(writer, sheet_name='SourceColumnsdiff', index=False)
        elif not df_added_source_columns_df.empty:
            df_added_source_columns_df.to_excel(writer, sheet_name='SourceColumnsdiff', index=False)
        elif not df_removed_source_columns_df.empty:
            df_removed_source_columns_df.to_excel(writer, sheet_name='SourceColumnsdiff', index=False)
        elif not df_changed_source_columns_df.empty:
            df_changed_source_columns_df.to_excel(writer, sheet_name='SourceColumnsdiff', index=False)
        else:
            logger.info("No Source Columns Difference Found")
            source_columns_diff_df = pd.DataFrame(columns=df_added_source_columns_df.columns.to_list())
            source_columns_diff_df.to_excel(writer, sheet_name='SourceColumnsdiff', index=False)


    # Get Hierarchies
    def get_hierarchies(spaceID):
        getHierarchies = client.service.getAllHierarchies(login_token, spaceID)
        return getHierarchies


    if Hierarchies_code in options_list:
        logger.info("Getting Hierarchies From " + space1 + " and " + space2)
        with concurrent.futures.ThreadPoolExecutor() as executor2:
            space1_hierarchies_response = executor2.submit(get_hierarchies, space1ID)
            space2_hierarchies_response = executor2.submit(get_hierarchies, space2ID)

        space1_hierarchies = space1_hierarchies_response.result()
        space2_hierarchies = space2_hierarchies_response.result()

        if not space1_hierarchies:
            space1_hierarchies = []

        if not space2_hierarchies:
            space2_hierarchies = []

        space1_hierarchies_df = pd.DataFrame({'Hierarchies': space1_hierarchies, 'Change Type': 'Removed'})
        space2_hierarchies_df = pd.DataFrame({'Hierarchies': space2_hierarchies, 'Change Type': 'Added'})

        logger.info("Comparing Hierarchies..")
        hierarchies_diff_df = pd.concat([space1_hierarchies_df, space2_hierarchies_df], ignore_index=True)
        hierarchies_diff_df.drop_duplicates(subset=['Hierarchies'], keep=False, inplace=True, ignore_index=True)
        hierarchies_diff_df.sort_values(by=['Hierarchies'], inplace=True)
        hierarchies_diff_df.to_excel(writer, sheet_name="Hierarchiesdiff", index=False)

        if hierarchies_diff_df.empty:
            logger.info("No Hierarchies Difference Found.")


    # Get Variables
    def get_variables(spaceID):
        variable_name = []
        variable_value = []
        variables_list = client.service.getVariablesForSpace(login_token, spaceID)
        for variable in variables_list:
            variable_name.append(variable['string'][0])
            variable_value.append(variable['string'][1])
        variables_df = pd.DataFrame({"Name": variable_name, "Value": variable_value})
        return variables_df


    # Variables
    if Varaibles_code in options_list:
        logger.info("Getting Variables From " + space1 + " and " + space2)
        variables_space1_df = get_variables(space1ID)
        variables_space2_df = get_variables(space2ID)
        logger.info("Comparing Variables..")
        # Comparing Variables

        variables_columns_list = variables_space2_df.columns.to_list()
        variables_space1_df['Change Type'] = "Old"
        variables_space2_df['Change Type'] = "New"

        # Creating Key Columns
        variables_space1_df["key"] = variables_space1_df["Name"]
        variables_space2_df["key"] = variables_space2_df["Name"]

        old_rows_variables = set(variables_space1_df["key"])
        new_rows_variables = set(variables_space2_df["key"])

        dropped_rows_variables = (old_rows_variables - new_rows_variables)
        added_rows_variables = (new_rows_variables - old_rows_variables)

        # Combining Data
        all_data_variables = pd.concat([variables_space1_df, variables_space2_df], ignore_index=True)
        all_data_variables.drop_duplicates(subset=variables_columns_list, keep='last', inplace=True)

        # Getting Duplicates
        dupp_rows_variables_list = all_data_variables[all_data_variables['key'].duplicated() == True][
            'key'].to_list()
        dupes_variables_df = all_data_variables[
            all_data_variables['key'].isin(dupp_rows_variables_list)]

        dupes_old_variables_df = dupes_variables_df[dupes_variables_df['Change Type'] == "Old"]
        dupes_new_variables_df = dupes_variables_df[dupes_variables_df['Change Type'] == "New"]
        # dropping version cols
        dupes_old_variables_df = dupes_old_variables_df.drop(columns=['Change Type'])
        dupes_new_variables_df = dupes_new_variables_df.drop(columns=['Change Type'])
        # Setting Index to Key
        dupes_old_variables_df.set_index('key', inplace=True)
        dupes_new_variables_df.set_index('key', inplace=True)

        # Combining both dupes
        df_all_changes_variables_df = pd.concat([dupes_old_variables_df, dupes_new_variables_df],
                                                axis='columns', keys=['Old', 'New'], join='outer')
        df_all_changes_variables_df = df_all_changes_variables_df.swaplevel(axis='columns')[
            dupes_new_variables_df.columns[0:]]
        df_changed_variables_df = df_all_changes_variables_df.groupby(level=0, axis=1).apply(
            lambda frame: frame.apply(report_diff, axis=1))
        df_changed_variables_df = df_changed_variables_df.reset_index()
        df_changed_variables_df = df_changed_variables_df[variables_columns_list]
        df_changed_variables_df['Change Type'] = "Changed"

        # Removed Rows
        df_removed_variables_df = all_data_variables[
            all_data_variables['key'].isin(dropped_rows_variables)]
        df_removed_variables_df = df_removed_variables_df.drop(columns=['Change Type', 'key'])
        df_removed_variables_df['Change Type'] = 'Removed'

        # Added Rows
        df_added_variables_df = all_data_variables[
            all_data_variables['key'].isin(added_rows_variables)]
        df_added_variables_df = df_added_variables_df.drop(columns=['Change Type', 'key'])
        df_added_variables_df['Change Type'] = 'Added'

        # Final DataFrame

        if not (
                df_changed_variables_df.empty or df_added_variables_df.empty or df_removed_variables_df.empty):
            variables_diff_df = pd.concat(
                [df_added_variables_df, df_removed_variables_df, df_changed_variables_df],
                ignore_index=True)
            variables_diff_df.to_excel(writer, sheet_name='Variablesdiff', index=False)
        elif not (df_changed_variables_df.empty or df_added_variables_df.empty):
            variables_diff_df = pd.concat(
                [df_added_variables_df, df_changed_variables_df], ignore_index=True)
            variables_diff_df.to_excel(writer, sheet_name='Variablesdiff', index=False)
        elif not (df_changed_variables_df.empty or df_removed_variables_df.empty):
            variables_diff_df = pd.concat(
                [df_changed_variables_df, df_removed_variables_df], ignore_index=True)
            variables_diff_df.to_excel(writer, sheet_name='Variablesdiff', index=False)
        elif not (df_added_variables_df.empty or df_removed_variables_df.empty):
            variables_diff_df = pd.concat(
                [df_added_variables_df, df_removed_variables_df], ignore_index=True)
            variables_diff_df.to_excel(writer, sheet_name='Variablesdiff', index=False)
        elif not df_added_variables_df.empty:
            df_added_variables_df.to_excel(writer, sheet_name='Variablesdiff', index=False)
        elif not df_removed_variables_df.empty:
            df_removed_variables_df.to_excel(writer, sheet_name='Variablesdiff', index=False)
        elif not df_changed_variables_df.empty:
            df_changed_variables_df.to_excel(writer, sheet_name='Variablesdiff', index=False)
        else:
            logger.info("No Varaible Difference Found")
            source_columns_diff_df = pd.DataFrame(columns=df_added_variables_df.columns.to_list())
            source_columns_diff_df.to_excel(writer, sheet_name='Variablesdiff', index=False)


    # Getting Directories
    # Getting Directories From Space1
    def get_directories1(directory1):
        content = client.service.getDirectoryContents(login_token, space1ID, directory1)
        if content['name'] == directory:
            folders_list1.append(directory)
            folders_last_modified1.append(content['lastModified'])
        if content['isDirectory']:
            try:
                children = content['children']['FileNode']
            except Exception:
                children = []
            if children:
                children_list1 = []
                for i in range(len(children)):
                    folders_list1.append(content['name'] + '/' + children[i]['name'])
                    folders_last_modified1.append(children[i]['lastModified'])
                    if children[i]['isDirectory']:
                        children_list1.append(directory1 + '/' + children[i]['name'])
                try:
                    if children_list1:
                        with concurrent.futures.ThreadPoolExecutor() as executor3:
                            executor3.map(get_directories1, children_list1)
                except Exception as e1:
                    logger.debug(e1)
                    logger.debug("Executing in Normal Loop: ")
                    for j in range(len(children_list1)):
                        get_directories1(directory1 + '/' + children[j]['name'])


    # Getting Directories From Space2
    def get_directories2(directory1):
        content = client.service.getDirectoryContents(login_token, space2ID, directory1)
        if content['name'] == directory:
            folders_list2.append(directory)
            folders_last_modified2.append(content['lastModified'])
        if content['isDirectory']:
            try:
                children = content['children']['FileNode']
            except Exception:
                children = []
            if children:
                children_list2 = []
                for i in range(len(children)):
                    folders_list2.append(content['name'] + '/' + children[i]['name'])
                    folders_last_modified2.append(children[i]['lastModified'])
                    if children[i]['isDirectory']:
                        children_list2.append(directory1 + '/' + children[i]['name'])
                try:
                    if children_list2:
                        with concurrent.futures.ThreadPoolExecutor() as executor3:
                            executor3.map(get_directories2, children_list2)
                except Exception as e2:
                    logger.debug(e2)
                    logger.debug("Executing in Normal Loop: ")
                    for j in range(len(children_list2)):
                        get_directories2(directory1 + '/' + children[j]['name'])


    if FolderPermissions_code in options_list or Catalog_code in options_list:
        directories_start_time = time.perf_counter()
        if not directory:
            exit_code = 2
            raise RuntimeError('Directory is not given...Exiting')
            sys.exit(exit_code)

        logger.info("Getting Directories From " + space1 + " and " + space2)
        logger.info(".....Waiting......")
        get_directories1(directory)
        get_directories2(directory)
        logger.info(
            'Getting Directories Completed...' + ' Time Taken: ' + str(time.perf_counter() - directories_start_time))
        logger.info(".............")


        def get_catalog_folders(folders_list, folders_last_modified):
            viz_reports = []
            viz_reports_last_modified = []
            designer_reports = []
            designer_reports_last_modified = []
            dashboards = []
            dashboards_last_modified = []
            images_list = []
            images_last_modified = []
            directory_list = []
            for folder in range(len(folders_list)):
                if '.dashlet' in folders_list[folder]:
                    viz_reports.append(folders_list[folder])
                    viz_reports_last_modified.append(folders_last_modified[folder])
                elif '.AdhocReport' in folders_list[folder]:
                    designer_reports.append(folders_list[folder])
                    designer_reports_last_modified.append(folders_last_modified[folder])
                elif '.page' in folders_list[folder]:
                    dashboards.append(folders_list[folder])
                    dashboards_last_modified.append(folders_last_modified[folder])
                elif '.png' in folders_list[folder]:
                    images_list.append(folders_list[folder])
                    images_last_modified.append(folders_last_modified[folder])
                else:
                    directory_list.append(folders_list[folder])
            return viz_reports, viz_reports_last_modified, designer_reports, designer_reports_last_modified, dashboards, \
                   dashboards_last_modified, images_list, images_last_modified, directory_list


        viz_reports1, viz_reports_last_modified1, designer_reports1, designer_reports_last_modified1, dashboards1, dashboards_last_modified1, \
        images_list1, images_last_modified1, directory_list1 = get_catalog_folders(folders_list1,
                                                                                   folders_last_modified1)

        viz_reports2, viz_reports_last_modified2, designer_reports2, designer_reports_last_modified2, dashboards2, dashboards_last_modified2, \
        images_list2, images_last_modified2, directory_list2 = get_catalog_folders(folders_list2,
                                                                                   folders_last_modified2)



    # Get Folder Permissions For Space1
    def get_directory_permissions(spaceID, dir1):
        result = client.service.getDirectoryPermissions(login_token, spaceID, dir1)
        d2 = {}
        grp_name = []
        can_view = []
        can_modify = []
        for k in range(len(result)):
            grp_name.append(result[k]['groupName'])
            can_view.append(result[k]['canView'])
            can_modify.append(result[k]['canModify'])
        d2['Directory'] = dir1
        d2['GroupName'] = grp_name
        d2['CanView'] = can_view
        d2['CanModify'] = can_modify
        df2 = pd.DataFrame(d2)
        return df2


    if FolderPermissions_code in options_list:
        folder_permissions_start_time = time.perf_counter()
        logger.info("Getting Folder Permissions From " + space1 + " and " + space2 + "...Please Wait")
        logger.info(".....Waiting......")
        folder_permissions_df1 = [get_directory_permissions(space1ID, folder) for folder in directory_list1]
        folder_permissions_df2 = [get_directory_permissions(space2ID, folder) for folder in directory_list2]
        directory_permissions_sheet1 = pd.concat(folder_permissions_df1, ignore_index=True)
        directory_permissions_sheet2 = pd.concat(folder_permissions_df2, ignore_index=True)

        logger.info('Getting Folder Permissions Completed....' + ' Time Taken: ' + str(
            time.perf_counter() - folder_permissions_start_time))
        logger.info('.........')
        logger.info("Comparing Folder Permissions..")
        directory_permissions_columns = directory_permissions_sheet2.columns.to_list()
        directory_permissions_sheet1['Change Type'] = "Old"
        directory_permissions_sheet2['Change Type'] = "New"
        directory_permissions_sheet1["key"] = directory_permissions_sheet1['Directory'] + "_" + \
                                              directory_permissions_sheet1['GroupName']
        directory_permissions_sheet2["key"] = directory_permissions_sheet2['Directory'] + "_" + \
                                              directory_permissions_sheet2['GroupName']
        old_rows_directory_permissions = set(directory_permissions_sheet1["key"])
        new_rows_directory_permissions = set(directory_permissions_sheet2["key"])
        dropped_rows_directory_permissions = old_rows_directory_permissions - new_rows_directory_permissions
        added_rows_directory_permissions = new_rows_directory_permissions - old_rows_directory_permissions

        # All Data
        directory_permissions_alldata = pd.concat([directory_permissions_sheet1, directory_permissions_sheet2],
                                                  ignore_index=True)
        directory_permissions_alldata.drop_duplicates(subset=directory_permissions_columns, keep='last', inplace=True)

        # Getting Duplicates
        dupp_rows_directory_permissions = \
            directory_permissions_alldata[directory_permissions_alldata['key'].duplicated() == True]['key'].to_list()
        dupes_directory_permissions = directory_permissions_alldata[
            directory_permissions_alldata['key'].isin(dupp_rows_directory_permissions)]

        dupes_old_directory_permissions = dupes_directory_permissions[
            dupes_directory_permissions['Change Type'] == "Old"]
        dupes_new_directory_permissions = dupes_directory_permissions[
            dupes_directory_permissions['Change Type'] == "New"]

        # dropping version cols

        dupes_old_directory_permissions = dupes_old_directory_permissions.drop(columns=['Change Type'])
        dupes_new_directory_permissions = dupes_new_directory_permissions.drop(columns=['Change Type'])

        # Setting Index to Key
        dupes_old_directory_permissions.set_index('key', inplace=True)
        dupes_new_directory_permissions.set_index('key', inplace=True)

        # Combining both dupes, Finding Changes
        df_all_changes_directory_permissions = pd.concat(
            [dupes_old_directory_permissions, dupes_new_directory_permissions],
            axis='columns', keys=['Old', 'New'], join='outer')
        df_all_changes_directory_permissions = df_all_changes_directory_permissions.swaplevel(axis='columns')[
            dupes_new_directory_permissions.columns[0:]]
        df_changed_directory_permissions = df_all_changes_directory_permissions.groupby(level=0, axis=1).apply(
            lambda frame: frame.apply(report_diff, axis=1))
        df_changed_directory_permissions = df_changed_directory_permissions.reset_index()
        df_changed_directory_permissions = df_changed_directory_permissions[directory_permissions_columns]
        df_changed_directory_permissions['Change Type'] = "Changed"

        # Removed Rows
        df_removed_directory_permissions = directory_permissions_alldata[
            directory_permissions_alldata['key'].isin(dropped_rows_directory_permissions)]
        df_removed_directory_permissions = df_removed_directory_permissions.drop(columns=['Change Type', 'key'])
        df_removed_directory_permissions['Change Type'] = 'Removed'

        # Added Rows
        df_added_directory_permissions = directory_permissions_alldata[
            directory_permissions_alldata['key'].isin(added_rows_directory_permissions)]
        df_added_directory_permissions = df_added_directory_permissions.drop(columns=['Change Type', 'key'])
        df_added_directory_permissions['Change Type'] = 'Added'

        # Final DataFrame
        if not (
                df_changed_directory_permissions.empty or df_added_directory_permissions.empty or df_removed_directory_permissions.empty):
            directory_permissions_diff_df = pd.concat(
                [df_added_directory_permissions, df_removed_directory_permissions, df_changed_directory_permissions],
                ignore_index=True)
            directory_permissions_diff_df.to_excel(writer, sheet_name='FolderPermissionsdiff', index=False)
        elif not (df_changed_directory_permissions.empty or df_added_directory_permissions.empty):
            directory_permissions_diff_df = pd.concat(
                [df_added_directory_permissions, df_changed_directory_permissions], ignore_index=True)
            directory_permissions_diff_df.to_excel(writer, sheet_name='FolderPermissionsdiff', index=False)
        elif not (df_changed_directory_permissions.empty or df_removed_directory_permissions.empty):
            directory_permissions_diff_df = pd.concat(
                [df_changed_directory_permissions, df_removed_directory_permissions], ignore_index=True)
            directory_permissions_diff_df.to_excel(writer, sheet_name='FolderPermissionsdiff', index=False)
        elif not (df_added_directory_permissions.empty or df_removed_directory_permissions.empty):
            directory_permissions_diff_df = pd.concat(
                [df_added_directory_permissions, df_removed_directory_permissions], ignore_index=True)
            directory_permissions_diff_df.to_excel(writer, sheet_name='FolderPermissionsdiff', index=False)
        elif not df_added_directory_permissions.empty:
            df_added_directory_permissions.to_excel(writer, sheet_name='FolderPermissionsdiff', index=False)
        elif not df_removed_directory_permissions.empty:
            df_removed_directory_permissions.to_excel(writer, sheet_name='FolderPermissionsdiff', index=False)
        elif not df_changed_directory_permissions.empty:
            df_changed_directory_permissions.to_excel(writer, sheet_name='FolderPermissionsdiff', index=False)
        else:
            logger.info("No Folder Permissions Difference Found")
            directory_permissions_diff_df = pd.DataFrame(columns=df_added_directory_permissions.columns.to_list())
            directory_permissions_diff_df.to_excel(writer, sheet_name='FolderPermissionsdiff', index=False)


    # Getting Subject Areas
    def get_customsubjectAreas(spaceID):
        subjectareas_list = client.service.listCustomSubjectAreas(login_token, spaceID)
        return subjectareas_list


    # Getting Subject Area Permissions
    def get_subjectarepermissions(space_ID, name1):
        d1 = {}
        permissions = client.service.getSubjectAreaPermissions(login_token, space_ID, name1)
        d1['SubjectArea'] = name1
        d1['Permissions'] = permissions
        df1 = pd.DataFrame(d1)
        return df1


    if SubjectAreaPermissions_code in options_list:
        logger.info("Getting Subject Areas From " + space1 + " and " + space2)
        subject_areas_list1 = get_customsubjectAreas(space1ID)
        subject_areas_list2 = get_customsubjectAreas(space2ID)
        logger.info('Getting Subject Areas Completed...')
        logger.info('..........')

        logger.info('Getting Subject Area Permissions Started.....')

        logger.info("Getting Subject Area Permissions....")
        subject_areas_dataframes_list1 = [get_subjectarepermissions(space1ID, subject_area1) for subject_area1 in
                                          subject_areas_list1]
        subject_areas_dataframes_list2 = [get_subjectarepermissions(space2ID, subject_area2) for subject_area2 in
                                          subject_areas_list2]
        subject_areas_dataframe1 = pd.concat(subject_areas_dataframes_list1, ignore_index=True)
        subject_areas_dataframe2 = pd.concat(subject_areas_dataframes_list2, ignore_index=True)

        logger.info('Getting Subject Area Permissions Completed...')
        logger.info('Comparing Subject Area Permissions..')
        # Comparing Subject Area Permissions
        subject_areas_columns = subject_areas_dataframe2.columns.to_list()
        subject_areas_dataframe1['Change Type'] = "Removed"
        subject_areas_dataframe2['Change Type'] = "Added"
        subject_areas_diff_df = pd.concat([subject_areas_dataframe1, subject_areas_dataframe2], ignore_index=True)
        subject_areas_diff_df.drop_duplicates(subset=subject_areas_columns, keep=False, inplace=True)
        subject_areas_diff_df.to_excel(writer, sheet_name='SubjectAreaPermissionsdiff', index=False)
        if subject_areas_diff_df.empty:
            logger.info("No Subject Area Permissions Difference Found.")

    logger.info('..........')
    if Catalog_code in options_list:
        logger.info("Comparing Catalog..")
        viz_reports1_name = [report.split('/')[-1].partition('.viz.dashlet')[0].strip() for report in viz_reports1]
        designer_reports1_name = [report.split('/')[-1].partition('.AdhocReport')[0].strip() for report in designer_reports1]
        dashboards1_name = [report.split('/')[-1].partition('.page')[0].strip() for report in dashboards1]
        images_list1_name = [report.split('/')[-1].strip() for report in images_list1]
        viz_reports2_name = [report.split('/')[-1].partition('.viz.dashlet')[0].strip() for report in viz_reports2]
        designer_reports2_name = [report.split('/')[-1].partition('.AdhocReport')[0].strip() for report in designer_reports2]
        dashboards2_name = [report.split('/')[-1].partition('.page')[0].strip() for report in dashboards2]
        images_list2_name = [report.split('/')[-1].strip() for report in images_list2]

        viz_reports_sheet1 = pd.DataFrame(
            {'VisualizerReportPath': viz_reports1, 'Name': viz_reports1_name, 'lastModified': viz_reports_last_modified1})
        viz_reports_sheet2 = pd.DataFrame(
            {'VisualizerReportPath': viz_reports2,'Name': viz_reports2_name,'lastModified': viz_reports_last_modified2})
        designer_reports_sheet1 = pd.DataFrame(
            {'DesignerReports': designer_reports1, 'Name': designer_reports1_name,'lastModified': designer_reports_last_modified1})
        designer_reports_sheet2 = pd.DataFrame(
            {'DesignerReports': designer_reports2, 'Name': designer_reports2_name, 'lastModified': designer_reports_last_modified2})
        dashboards_sheet1 = pd.DataFrame({'Dashboards': dashboards1, 'Name':dashboards1_name, 'lastModified': dashboards_last_modified1})
        dashboards_sheet2 = pd.DataFrame({'Dashboards': dashboards2, 'Name':dashboards2_name, 'lastModified': dashboards_last_modified2})
        images_list_sheet1 = pd.DataFrame({'Images': images_list1, 'lastModified': images_last_modified1})
        images_list_sheet2 = pd.DataFrame({'Images': images_list2, 'lastModified': images_last_modified2})

        # Comparing Viz Reports
        viz_reports_columns = viz_reports_sheet2.columns.to_list()
        viz_reports_sheet1['Change Type'] = "Removed"
        viz_reports_sheet2['Change Type'] = "Added"
        viz_reports_diff_df = pd.concat([viz_reports_sheet1, viz_reports_sheet2], ignore_index=True)
        viz_reports_diff_df.drop_duplicates(subset=viz_reports_columns[0], keep=False, inplace=True)
        if viz_reports_diff_df.empty:
            logger.info("No Visualizer Reports Difference Found")

        # Comparing Desinger Reports
        designer_reports_columns = designer_reports_sheet2.columns.to_list()
        designer_reports_sheet1['Change Type'] = "Removed"
        designer_reports_sheet2['Change Type'] = "Added"
        designer_reports_diff_df = pd.concat([designer_reports_sheet1, designer_reports_sheet2], ignore_index=True)
        designer_reports_diff_df.drop_duplicates(subset=designer_reports_columns[0], keep=False, inplace=True)

        if designer_reports_diff_df.empty:
            logger.info("No Designer Reports Difference Found")

        # Comparing Dashboards
        dashboards_columns = dashboards_sheet2.columns.to_list()
        dashboards_sheet1['Change Type'] = "Removed"
        dashboards_sheet2['Change Type'] = "Added"
        dashboards_diff_df = pd.concat([dashboards_sheet1, dashboards_sheet2], ignore_index=True)
        dashboards_diff_df.drop_duplicates(subset=dashboards_columns[0], keep=False, inplace=True)

        if dashboards_diff_df.empty:
            logger.info("No Dashboards Difference Found")

        # Comparing Images
        images_columns = images_list_sheet2.columns.to_list()
        images_list_sheet1['Change Type'] = "Removed"
        images_list_sheet2['Change Type'] = "Added"
        images_diff_df = pd.concat([images_list_sheet1, images_list_sheet2], ignore_index=True)
        images_diff_df.drop_duplicates(subset=images_columns[0], keep=False, inplace=True)

        if images_diff_df.empty:
            logger.info("No Images Difference Found")

        dashboards_diff_df.to_excel(writer, sheet_name='Dashboardsdiff', index=False)
        viz_reports_diff_df.to_excel(writer, sheet_name='VisualizerReportsdiff', index=False)
        designer_reports_diff_df.to_excel(writer, sheet_name='DesignerReportsdiff', index=False)
        images_diff_df.to_excel(writer, sheet_name='Imagesdiff', index=False)

    logger.info("Exporting the results to spreadsheet")

    writer.close()

    logger.info('Exporting the results to spreadsheet completed....')
    if login_token:
        logger.info('Logout from Birst ...')
        logger.info('Hurray!!! Tool Ran Successfully')
        logger.info('Please Check the results in the spreadsheet Created')
        try:
            client.service.Logout(login_token)
        except Exception as e:
            logger.error('Logout from Birst failed')

except Exception as e:
    logger.error('An unexpected error occurred while processing ')
    logger.error(e)
    exc_type, exc_obj, exc_tb = sys.exc_info()
    logger.debug('line number = ' + str(exc_tb.tb_lineno))
    if login_token:
        logger.info('on error: Logout from Birst ...')
        try:
            client.service.Logout(login_token)
        except Exception as e:
            logger.error('on error: Logout from Birst failed')

os.system('pause')
