from shareplum import Site
from shareplum import Office365
from shareplum.site import Version
from databricks.sdk.runtime import *


class SharepointConnector:
    '''
    SPconnector class to upload files to Sharepoint

    Attributes
    ----------
    site_path : str
        site path
    folder: str
        folder name
    username : str, optional
        username
    password : str, optional
        password


    Example
    -------
    folder = 'Shared Documents/Primary360/Journey_Performance/Engagement_Journeys/'
    sp = SharePointConnector(folder=folder)
    '''

    def __init__(self, site_path=None, folder=None, username=None, password=None):
        self.username = dbutils.secrets.get(scope = 'dbrks-sharepoint-svc-account', key = 'username') if username is None else username
        self.password = dbutils.secrets.get(scope = 'dbrks-sharepoint-svc-account', key = 'password') if password is None else password
        self.site_path = site_path if site_path is not None else 'https://teladocpa.sharepoint.com/sites/TDOCMarketingDS'
        self.authcookie = Office365('https://teladocpa.sharepoint.com', username=self.username, password=self.password).GetCookies()
        self.site = Site(self.site_path, version=Version.v365, authcookie=self.authcookie)
        self.folder= self.site.Folder(folder)

        print(f'Sharepoint connected to site: {self.site_path}')

    def upload_file(self, file_path=None, file_name=None):
        '''
        Upload file to sharepoint

        Parameters
        ----------
        file_path : str
            file path from where to upload the file (local)
        file_name : str
            file name we want


        Example
        -------

        folder = 'Shared Documents/Primary360/Journey_Performance/Engagement_Journeys/'
        sp = SPconnector(folder=folder)

        with pd.ExcelWriter('output/test.xlsx') as writer:  
           test.to_excel(writer, index=False, sheet_name='test')
           
        sp.upload_file(file_path='output/', file_name='test.xlsx')
        '''

        if file_path is None:
            file_path = ''

        with open(file_path + file_name, mode="rb") as file:
            fileContent = file.read()
        self.folder.upload_file(fileContent, file_name)
        print("File uploaded to Sharepoint!")


    def read_file(self, file_name):
        '''
        Read file from sharepoint

        Parameters
        ----------
        file_name : str
            file name

        Example
        -------
        folder = 'Shared Documents/Primary360/Journey_Performance/Engagement_Journeys/'
        sp = SPconnector(folder=folder)
        test = sp.read_file(file_name='test.xlsx')
        df = pd.read_excel(test)
        '''
    
    
        return self.folder.get_file(file_name)
    

    def delete_file(self, file_name):
        '''
        Delete file from sharepoint

        Parameters
        ----------
        file_name : str
            file name

        Example
        -------
        folder = 'Shared Documents/Primary360/Journey_Performance/Engagement_Journeys/'
        sp = SPconnector(folder=folder)
        sp.delete_file(file_name='test.xlsx')
        '''
    
        print('Deleting file!')
        return self.folder.delete_file(file_name)
    

    def delete_folder(self, folder_name):
        '''
        Delete folder from sharepoint

        Parameters
        ----------
        folder_name : str
            folder name

        Example
        -------
        folder = 'Shared Documents/Primary360/Journey_Performance/Engagement_Journeys/'
        sp = SPconnector(folder=folder)
        sp.delete_folder(folder_path=folder)
        '''
    
        print('Deleting folder!')
        return self.folder.delete_folder(folder_name)