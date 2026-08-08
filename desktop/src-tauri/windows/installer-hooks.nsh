!include LogicLib.nsh

!define PLATO_INSTALLER_GATE_PREFIX "Global\plato-agent-installer"
!define PLATO_INSTALLER_GATE_WAIT_MS 5000
!define PLATO_CONTROL_TIMEOUT_MS 60000
!define PLATO_SECURITY_ATTRIBUTES_SIZE 12

Var PlatoInstallerGate
Var PlatoInstallerGateResult
Var PlatoInstallerSid

!macro PLATO_INSTALLER_FUNCTIONS prefix
Function ${prefix}PlatoAcquireInstallerGate
  Push $0
  Push $1
  Push $2
  Push $3
  Push $4
  Push $5
  Push $6
  Push $7
  Push $8
  Push $9
  StrCpy $PlatoInstallerGate 0
  StrCpy $PlatoInstallerGateResult "security_failed"
  StrCpy $PlatoInstallerSid ""
  StrCpy $0 0
  StrCpy $1 0
  StrCpy $2 0
  StrCpy $3 0
  StrCpy $4 0
  StrCpy $5 0
  StrCpy $6 0
  StrCpy $7 0
  StrCpy $8 0
  StrCpy $9 0

  System::Call 'kernel32::GetCurrentProcess() p .r0'
  System::Call 'advapi32::OpenProcessToken(p r0, i 0x0008, *p .r1) i .r7'
  ${If} $7 = 0
    Goto installer_gate_done
  ${EndIf}
  System::Call 'advapi32::GetTokenInformation(p r1, i 1, p 0, i 0, *i .r2) i .r7'
  ${If} $2 = 0
    Goto installer_gate_done
  ${EndIf}
  System::Alloc $2
  Pop $3
  ${If} $3 = 0
    Goto installer_gate_done
  ${EndIf}
  System::Call 'advapi32::GetTokenInformation(p r1, i 1, p r3, i r2, *i .r2) i .r7'
  ${If} $7 = 0
    Goto installer_gate_done
  ${EndIf}
  System::Call '*$3(p .r4)'
  System::Call 'advapi32::ConvertSidToStringSidW(p r4, *p .r5) i .r7'
  ${If} $7 = 0
    Goto installer_gate_done
  ${EndIf}
  System::Call 'kernel32::lstrcpynW(w .r6, p r5, i ${NSIS_MAX_STRLEN}) p'
  StrCpy $PlatoInstallerSid $6
  StrCpy $6 "${PLATO_INSTALLER_GATE_PREFIX}-$PlatoInstallerSid"
  StrCpy $7 "O:$PlatoInstallerSid"
  StrCpy $7 "$7D:P(A;;GA;;;$PlatoInstallerSid)"
  System::Call 'advapi32::ConvertStringSecurityDescriptorToSecurityDescriptorW(w r7, i 1, *p .r8, p 0) i .r2'
  ${If} $2 = 0
    Goto installer_gate_done
  ${EndIf}
  System::Call '*(i ${PLATO_SECURITY_ATTRIBUTES_SIZE}, p r8, i 0) p .r9'
  ${If} $9 = 0
    System::Call 'kernel32::LocalFree(p r8)'
    StrCpy $8 0
    Goto installer_gate_done
  ${EndIf}
  System::Call 'kernel32::SetLastError(i 0)'
  System::Call 'kernel32::CreateMutexW(p r9, i 1, w r6) p .r0 ?e'
  Pop $7
  System::Free $9
  StrCpy $9 0
  System::Call 'kernel32::LocalFree(p r8)'
  StrCpy $8 0
  ${If} $0 = 0
    StrCpy $PlatoInstallerGateResult "create_failed:$7"
    Goto installer_gate_done
  ${EndIf}

  ${If} $7 = 183
    System::Call 'advapi32::GetSecurityInfo(p r0, i 6, i 1, *p .r8, p 0, p 0, p 0, *p .r9) i .r7'
    ${If} $7 != 0
    ${OrIf} $8 = 0
    ${OrIf} $9 = 0
      ${If} $9 != 0
        System::Call 'kernel32::LocalFree(p r9)'
        StrCpy $9 0
      ${EndIf}
      Goto installer_gate_done
    ${EndIf}
    System::Call 'advapi32::EqualSid(p r4, p r8) i .r7'
    System::Call 'kernel32::LocalFree(p r9)'
    StrCpy $9 0
    ${If} $7 = 0
      Goto installer_gate_done
    ${EndIf}
    System::Call 'kernel32::WaitForSingleObject(p r0, i ${PLATO_INSTALLER_GATE_WAIT_MS}) i .r7'
    ${If} $7 != 0
    ${AndIf} $7 != 128
      StrCpy $PlatoInstallerGateResult "wait_failed:$7"
      Goto installer_gate_done
    ${EndIf}
  ${EndIf}
  StrCpy $PlatoInstallerGate $0
  StrCpy $0 0
  StrCpy $PlatoInstallerGateResult "ok"

  installer_gate_done:
  ${If} $0 != 0
    System::Call 'kernel32::CloseHandle(p r0)'
  ${EndIf}
  ${If} $5 != 0
    System::Call 'kernel32::LocalFree(p r5)'
  ${EndIf}
  ${If} $3 != 0
    System::Free $3
  ${EndIf}
  ${If} $1 != 0
    System::Call 'kernel32::CloseHandle(p r1)'
  ${EndIf}
  Pop $9
  Pop $8
  Pop $7
  Pop $6
  Pop $5
  Pop $4
  Pop $3
  Pop $2
  Pop $1
  Pop $0
  Push $PlatoInstallerGateResult
FunctionEnd

Function ${prefix}PlatoReleaseInstallerGate
  ${If} $PlatoInstallerGate != 0
    System::Call 'kernel32::ReleaseMutex(p $PlatoInstallerGate)'
    System::Call 'kernel32::CloseHandle(p $PlatoInstallerGate)'
    StrCpy $PlatoInstallerGate 0
  ${EndIf}
FunctionEnd

Function ${prefix}PlatoShutdownDaemons
  ${IfNot} ${FileExists} "$INSTDIR\platonic.exe"
    Return
  ${EndIf}
  nsExec::Exec /TIMEOUT=${PLATO_CONTROL_TIMEOUT_MS} '"$INSTDIR\platonic.exe" shutdown --workspace "$INSTDIR"'
  Pop $0
  ${If} $0 != 0
    DetailPrint "Plato Agent daemon shutdown failed: $0"
    Call ${prefix}PlatoReleaseInstallerGate
    Abort "Plato Agent has an active run or its local daemon could not be stopped."
  ${EndIf}
FunctionEnd
!macroend

!insertmacro PLATO_INSTALLER_FUNCTIONS ""
!insertmacro PLATO_INSTALLER_FUNCTIONS "un."

Function .onInstFailed
  Call PlatoReleaseInstallerGate
FunctionEnd

Function un.onUninstFailed
  Call un.PlatoReleaseInstallerGate
FunctionEnd

!macro NSIS_HOOK_PREINSTALL
  Call PlatoAcquireInstallerGate
  Pop $0
  ${If} $0 != "ok"
    Abort "Another Plato Agent installation, update, or daemon start is in progress."
  ${EndIf}
  !insertmacro CheckIfAppIsRunning "${MAINBINARYNAME}.exe" "${PRODUCTNAME}"
  Call PlatoShutdownDaemons
!macroend

!macro NSIS_HOOK_POSTINSTALL
  Call PlatoReleaseInstallerGate
!macroend

!macro NSIS_HOOK_PREUNINSTALL
  Call un.PlatoAcquireInstallerGate
  Pop $0
  ${If} $0 != "ok"
    Abort "Another Plato Agent installation, update, or daemon start is in progress."
  ${EndIf}
  !insertmacro CheckIfAppIsRunning "${MAINBINARYNAME}.exe" "${PRODUCTNAME}"
  Call un.PlatoShutdownDaemons
!macroend

!macro NSIS_HOOK_POSTUNINSTALL
  Call un.PlatoReleaseInstallerGate
!macroend
