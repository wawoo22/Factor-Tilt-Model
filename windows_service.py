import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import asyncio
import sys
import os
from pathlib import Path

# Add current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from complete_system_integration import CompleteFactorMonitoringSystem, SystemConfig
from dotenv import load_dotenv

class FactorMonitoringService(win32serviceutil.ServiceFramework):
    _svc_name_ = "FactorMonitoringService"
    _svc_display_name_ = "Factor Monitoring System"
    _svc_description_ = "Automated factor portfolio monitoring and rebalancing"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, '')
        )
        self.main()

    def main(self):
        try:
            # Load configuration
            load_dotenv('.env.production')
            
            config = SystemConfig(
                db_path=os.getenv('DB_PATH', 'factor_monitoring_production.db'),
                email_sender=os.getenv('FACTOR_EMAIL'),
                email_password=os.getenv('FACTOR_EMAIL_PASSWORD'),
                email_recipients=os.getenv('FACTOR_RECIPIENTS', '').split(','),
                tos_client_id=os.getenv('TOS_CLIENT_ID', ''),
                tos_refresh_token=os.getenv('TOS_REFRESH_TOKEN', ''),
                tos_account_id=os.getenv('TOS_ACCOUNT_ID', ''),
                portfolio_value=float(os.getenv('PORTFOLIO_VALUE', 1000000))
            )
            
            # Run the system
            asyncio.run(self.run_system(config))
            
        except Exception as e:
            servicemanager.LogErrorMsg(f"Service error: {e}")

    async def run_system(self, config):
        factor_system = CompleteFactorMonitoringSystem(config)
        await factor_system.start_system()
        
        # Keep running until service stop
        while win32event.WaitForSingleObject(self.hWaitStop, 1000) != win32event.WAIT_OBJECT_0:
            # Process scheduled tasks
            await asyncio.sleep(60)

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(FactorMonitoringService)
