#!/bin/bash
# Factor Monitoring System - Master Command Launcher
# Usage: factor [command]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

show_banner() {
    echo -e "${BLUE}"
    echo "╔═══════════════════════════════════════════════════════╗"
    echo "║     📊 FACTOR MONITORING SYSTEM v1.0                 ║"
    echo "╚═══════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

show_help() {
    show_banner
    echo "Usage: factor [command]"
    echo ""
    echo "Available commands:"
    echo ""
    echo "  ${GREEN}run${NC}              - Run factor analysis (Yahoo Finance)"
    echo "  ${GREEN}schwab-enhanced${NC}  - Run with Schwab API + portfolio positions"
    echo "  ${GREEN}test${NC}             - Run system diagnostics"
    echo "  ${GREEN}email${NC}            - Test email configuration"
    echo "  ${GREEN}schwab${NC}           - Test Schwab API connection"
    echo "  ${GREEN}data${NC}             - Collect factor data only"
    echo "  ${GREEN}dashboard${NC}        - Start monitoring dashboard"
    echo "  ${GREEN}setup${NC}            - Setup environment and database"
    echo "  ${GREEN}status${NC}           - Check system status"
    echo "  ${GREEN}help${NC}             - Show this help message"
    echo ""
    echo "Examples:"
    echo "  factor run                # Standard analysis (Yahoo Finance)"
    echo "  factor schwab-enhanced    # With Schwab portfolio tracking"
    echo "  factor test               # Test all system components"
    echo ""
}

run_analysis() {
    echo -e "${BLUE}🚀 Running Factor Analysis...${NC}"
    python3 minimal_factor_system.py
}

run_schwab_enhanced() {
    echo -e "${BLUE}🔐 Running Schwab-Enhanced Analysis...${NC}"
    python3 schwab_factor_system.py
}

run_diagnostics() {
    echo -e "${BLUE}🔍 Running System Diagnostics...${NC}"
    python3 run_diagnostics.py
}

test_email() {
    echo -e "${BLUE}📧 Testing Email Configuration...${NC}"
    python3 test_email.py
}

test_schwab() {
    echo -e "${BLUE}🔐 Testing Schwab API...${NC}"
    python3 test_schwab_connection.py
}

collect_data() {
    echo -e "${BLUE}📊 Collecting Factor Data...${NC}"
    python3 factor_data_collector.py
}

start_dashboard() {
    echo -e "${BLUE}📈 Starting Monitoring Dashboard...${NC}"
    echo "Dashboard will be available at: http://localhost:8050"
    python3 monitoring_dashboard.py
}

setup_system() {
    echo -e "${BLUE}🔧 Setting up Factor Monitoring System...${NC}"
    
    if [ ! -f .env ]; then
        echo -e "${YELLOW}⚠️  .env file not found${NC}"
        echo "Would you like to create it now? (y/n)"
        read -r response
        if [[ "$response" =~ ^[Yy]$ ]]; then
            echo "Creating .env file..."
            cat > .env << 'ENVEOF'
# Factor Monitoring System Configuration

# Email Settings (Required)
FACTOR_EMAIL=your_email@gmail.com
FACTOR_EMAIL_PASSWORD=your_app_password
FACTOR_RECIPIENTS=recipient@example.com

# Schwab API Settings (Optional)
SCHWAB_CLIENT_ID=
SCHWAB_CLIENT_SECRET=
SCHWAB_REFRESH_TOKEN=

# Portfolio Settings
PORTFOLIO_VALUE=1000000
ENVEOF
            echo -e "${GREEN}✅ .env file created${NC}"
            echo "Please edit .env and add your credentials"
        fi
    fi
    
    echo "Setting up database..."
    python3 setup_database.py
    
    echo "Running diagnostics..."
    python3 run_diagnostics.py
}

show_status() {
    show_banner
    echo -e "${BLUE}📊 System Status${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    if [ -f .env ]; then
        echo -e "✅ Environment file: ${GREEN}Found${NC}"
    else
        echo -e "❌ Environment file: ${RED}Missing${NC}"
    fi
    
    if [ -f minimal_factor_data.db ]; then
        SIZE=$(ls -lh minimal_factor_data.db | awk '{print $5}')
        echo -e "✅ Database: ${GREEN}Found${NC} ($SIZE)"
    else
        echo -e "⚠️  Database: ${YELLOW}Not created yet${NC}"
    fi
    
    if [ -n "$VIRTUAL_ENV" ]; then
        echo -e "✅ Virtual env: ${GREEN}Active${NC} (${VIRTUAL_ENV##*/})"
    else
        echo -e "⚠️  Virtual env: ${YELLOW}Not active${NC}"
    fi
    
    PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
    echo -e "✅ Python: ${GREEN}$PYTHON_VERSION${NC}"
    
    if [ -f minimal_factor_data.db ]; then
        LAST_RUN=$(sqlite3 minimal_factor_data.db "SELECT MAX(date) FROM factor_data" 2>/dev/null || echo "Never")
        echo -e "📅 Last data collection: ${BLUE}$LAST_RUN${NC}"
    fi
    
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

# Main command handler
case "${1:-run}" in
    run|r)
        run_analysis
        ;;
    schwab-enhanced|schwab-full|portfolio)
        run_schwab_enhanced
        ;;
    test|t|diagnostic|diagnostics)
        run_diagnostics
        ;;
    email|e)
        test_email
        ;;
    schwab|s|api)
        test_schwab
        ;;
    data|d|collect)
        collect_data
        ;;
    dashboard|dash|monitor|m)
        start_dashboard
        ;;
    setup|install|init)
        setup_system
        ;;
    status|stat|info)
        show_status
        ;;
    help|h|-h|--help)
        show_help
        ;;
    *)
        echo -e "${RED}❌ Unknown command: $1${NC}"
        echo ""
        show_help
        exit 1
        ;;
esac
