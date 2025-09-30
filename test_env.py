# test_env.py - Test environment variable loading
import os
from dotenv import load_dotenv

def test_environment_variables():
    """Test that environment variables are loaded correctly"""
    
    print("🧪 ENVIRONMENT VARIABLE TEST")
    print("=" * 50)
    
    # Step 1: Load the .env file
    print("📁 Loading .env file...")
    load_dotenv()  # This reads the .env file
    print("✅ .env file loaded")
    
    # Step 2: Test each variable
    print("\n🔍 Checking environment variables:")
    
    # Email settings
    email = os.getenv('FACTOR_EMAIL')
    password = os.getenv('FACTOR_EMAIL_PASSWORD') 
    recipients = os.getenv('FACTOR_RECIPIENTS')
    
    # Display results (safely)
    print(f"📧 FACTOR_EMAIL: {email if email else '❌ NOT SET'}")
    
    if password:
        # Show password length and first/last chars for verification
        masked = password[0] + '*' * (len(password)-2) + password[-1]
        print(f"🔑 FACTOR_EMAIL_PASSWORD: {masked} (length: {len(password)})")
    else:
        print("🔑 FACTOR_EMAIL_PASSWORD: ❌ NOT SET")
    
    print(f"📮 FACTOR_RECIPIENTS: {recipients if recipients else '❌ NOT SET'}")
    
    # Optional settings
    portfolio_value = os.getenv('PORTFOLIO_VALUE')
    print(f"💰 PORTFOLIO_VALUE: {portfolio_value if portfolio_value else 'Not set (will use default)'}")
    
    # Step 3: Validate critical variables
    print("\n✅ VALIDATION:")
    
    required_vars = ['FACTOR_EMAIL', 'FACTOR_EMAIL_PASSWORD']
    missing_vars = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ MISSING REQUIRED VARIABLES: {', '.join(missing_vars)}")
        print("\n🔧 TO FIX:")
        print("1. Check your .env file exists")
        print("2. Make sure variable names are spelled correctly")
        print("3. Make sure there are no spaces around = signs")
        return False
    else:
        print("✅ All required variables found!")
        return True

def test_email_format():
    """Test if email addresses are valid format"""
    load_dotenv()
    
    email = os.getenv('FACTOR_EMAIL')
    if not email:
        return False
    
    # Basic email validation
    if '@' not in email or '.' not in email:
        print(f"⚠️  WARNING: '{email}' doesn't look like a valid email")
        return False
    
    print(f"✅ Email format looks good: {email}")
    return True

def test_password_format():
    """Test if app password looks correct"""
    load_dotenv()
    
    password = os.getenv('FACTOR_EMAIL_PASSWORD')
    if not password:
        return False
    
    # Remove spaces for testing
    clean_password = password.replace(' ', '')
    
    if len(clean_password) != 16:
        print(f"⚠️  WARNING: App password should be 16 characters, got {len(clean_password)}")
        print("   Make sure you copied the full app password from Google")
        return False
    
    print("✅ App password length looks correct (16 characters)")
    return True

def main():
    """Run all tests"""
    print("🚀 STARTING ENVIRONMENT TESTS")
    print("=" * 60)
    
    # Test 1: Load variables
    env_loaded = test_environment_variables()
    
    if not env_loaded:
        print("\n💥 CRITICAL ERROR: Environment variables not loaded properly")
        print("❌ Cannot proceed with system setup")
        return False
    
    print("\n" + "=" * 60)
    
    # Test 2: Validate formats  
    email_valid = test_email_format()
    password_valid = test_password_format()
    
    print("\n" + "=" * 60)
    print("📋 FINAL SUMMARY:")
    
    if env_loaded and email_valid and password_valid:
        print("🎉 ALL TESTS PASSED!")
        print("✅ Environment variables are configured correctly")
        print("✅ Ready to proceed with Factor Monitoring System setup")
        return True
    else:
        print("❌ SOME TESTS FAILED")
        print("🔧 Please fix the issues above before proceeding")
        return False

if __name__ == "__main__":
    main()
