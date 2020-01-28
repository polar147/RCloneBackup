On Error Resume Next 
 
quotes = """"
strComputer = "." 
Set objWMIService = GetObject("winmgmts:" & "{impersonationLevel=impersonate}!\\" & strComputer & "\root\cimv2") 
 
Set colItems = objWMIService.ExecQuery("Select * from Win32_MappedLogicalDisk") 
 
 dict = "{"
 
For Each objItem in colItems 
    dict = dict & quotes & objItem.DeviceID & quotes & " : " & quotes & objItem.ProviderName & quotes & ","
   
Next
dict = dict & "}"
dict = Replace(dict, ",}","}")
Wscript.Echo dict