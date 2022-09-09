from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive



#%% Tę częsc skopiowac i dodac na koncu kodu (przyklad z afisz_teatralny)

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   

upload_file_list = [f"afisz_teatralny_{datetime.today().date()}.xlsx", f'afisz_teatralny_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  
    




#%% Instrukcja:

#1. Zaimportuj biblioteki do pliku z kodem: 
    from pydrive.auth import GoogleAuth
    from pydrive.drive import GoogleDrive
    
#2. Podmień nazwy plików w liscie upload_file_list