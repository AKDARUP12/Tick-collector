; Inno Setup script — NIFTY Studio installer
; Builds NIFTY_Setup.exe: installs the GUI app per-user (no admin), creates
; Desktop + Start Menu shortcuts, full uninstaller. Data folder is left intact.

[Setup]
AppName=NIFTY Studio
AppVersion=2.2
AppPublisher=arrow_broker_orderflow
DefaultDirName={localappdata}\NIFTYStudio
DefaultGroupName=NIFTY Studio
PrivilegesRequired=lowest
OutputDir=.
OutputBaseFilename=NIFTY_Setup
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
UninstallDisplayIcon={app}\NIFTY_Studio.exe
DisableProgramGroupPage=yes

[Tasks]
Name: "desktopicon"; Description: "Create a &desktop shortcut"; GroupDescription: "Shortcuts:"

[Files]
Source: "NIFTY_Studio.exe"; DestDir: "{app}"; Flags: ignoreversion
Source: "NIFTY_ParquetViewer.exe"; DestDir: "{app}"; Flags: ignoreversion
Source: ".env.example"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\NIFTY Studio"; Filename: "{app}\NIFTY_Studio.exe"
Name: "{group}\NIFTY Parquet Viewer"; Filename: "{app}\NIFTY_ParquetViewer.exe"
Name: "{userdesktop}\NIFTY Studio"; Filename: "{app}\NIFTY_Studio.exe"; Tasks: desktopicon

[Run]
Filename: "{app}\NIFTY_Studio.exe"; Description: "Launch NIFTY Studio"; Flags: nowait postinstall skipifsilent
