@echo off
setlocal

set "ROOT_DIR=%~dp0.."
pushd "%ROOT_DIR%"

if not "%GRAALVM_HOME%"=="" (
  set "JAVA_HOME=%GRAALVM_HOME%"
  set "PATH=%GRAALVM_HOME%\bin;%PATH%"
) else (
  where native-image.cmd >NUL 2>&1
  if errorlevel 1 (
    echo Please set GRAALVM_HOME or ensure native-image is on PATH
    popd
    exit /b 1
  )
)

if not exist target\native-tmp\.tmp mkdir target\native-tmp\.tmp
if not exist target\native-image mkdir target\native-image

call lein with-profile -dev,+release uberjar
if errorlevel 1 (
  popd
  exit /b %errorlevel%
)

set "NATIVE_IMAGE_HEAP=%XIA_NATIVE_IMAGE_HEAP%"
if "%NATIVE_IMAGE_HEAP%"=="" set "NATIVE_IMAGE_HEAP=10g"

set "PGO_FLAG="
if not "%XIA_NATIVE_PGO_PROFILE%"=="" set "PGO_FLAG=--pgo=%XIA_NATIVE_PGO_PROFILE%"

set "NATIVE_IMAGE_CMD=native-image.cmd"
where native-image.cmd >NUL 2>&1
if errorlevel 1 set "NATIVE_IMAGE_CMD=native-image"

call %NATIVE_IMAGE_CMD% ^
  --verbose ^
  --future-defaults=all ^
  -H:+AddAllCharsets ^
  -Djava.awt.headless=true ^
  "-J-Xmx%NATIVE_IMAGE_HEAP%" ^
  %PGO_FLAG% ^
  -H:TempDirectory=target/native-tmp ^
  -H:Path=target/native-image ^
  -H:NativeLinkerOption=legacy_stdio_definitions.lib ^
  -jar target/xia.jar ^
  -H:Name=xia

set "EXIT_CODE=%ERRORLEVEL%"
popd
exit /b %EXIT_CODE%
