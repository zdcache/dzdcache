@echo off
setlocal
set "APP_HOME=%~dp0"
set "WRAPPER_JAR=%APP_HOME%gradle\wrapper\gradle-wrapper.jar"
if not exist "%WRAPPER_JAR%" (
  echo Fetching official Gradle 9.7.0 wrapper JAR ^(first run only^)...
  powershell -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='Stop'; $u='https://services.gradle.org/distributions/gradle-9.7.0-wrapper.jar'; $f='%WRAPPER_JAR%'; $tmp=$f+'.tmp'; try { Invoke-WebRequest -Uri $u -OutFile $tmp; $actual=(Get-FileHash $tmp -Algorithm SHA256).Hash.ToLower(); if ($actual -ne 'd81e0f23ade952b35e55333dd5f1821585e887c6d24305aeea2fbc8dad564b95') { throw 'Gradle wrapper SHA256 mismatch' }; Move-Item -Force $tmp $f } finally { Remove-Item -Force -ErrorAction SilentlyContinue $tmp }"
  if errorlevel 1 exit /b 1
)
if defined JAVA_HOME (
  set "JAVA_EXE=%JAVA_HOME%\bin\java.exe"
) else (
  set "JAVA_EXE=java.exe"
)
"%JAVA_EXE%" -Dorg.gradle.appname=gradlew -classpath "%WRAPPER_JAR%" org.gradle.wrapper.GradleWrapperMain %*
exit /b %ERRORLEVEL%
