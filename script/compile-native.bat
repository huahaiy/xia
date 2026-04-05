@echo off
setlocal

set "ROOT_DIR=%~dp0.."
set "STAGED_JAR=%TEMP%\xia-release-%RANDOM%%RANDOM%.jar"
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

call lein clean
if errorlevel 1 (
  if exist "%STAGED_JAR%" del /q "%STAGED_JAR%"
  popd
  exit /b %errorlevel%
)
call lein with-profile -dev,+release uberjar
if errorlevel 1 (
  if exist "%STAGED_JAR%" del /q "%STAGED_JAR%"
  popd
  exit /b %errorlevel%
)
copy /y target\xia.jar "%STAGED_JAR%" >NUL
if errorlevel 1 (
  if exist "%STAGED_JAR%" del /q "%STAGED_JAR%"
  popd
  exit /b %errorlevel%
)
call lein clean
if errorlevel 1 (
  if exist "%STAGED_JAR%" del /q "%STAGED_JAR%"
  popd
  exit /b %errorlevel%
)

if not exist target\native-tmp\.tmp mkdir target\native-tmp\.tmp
if not exist target\native-image mkdir target\native-image

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
  -jar "%STAGED_JAR%" ^
  -H:Name=xia

set "EXIT_CODE=%ERRORLEVEL%"
if exist "%STAGED_JAR%" del /q "%STAGED_JAR%"
popd
exit /b %EXIT_CODE%
