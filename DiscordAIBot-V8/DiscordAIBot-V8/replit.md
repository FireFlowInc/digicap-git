# DigiCap - Islamic Economy Discord Bot

## Overview

DigiCap is a Discord bot that simulates an Islamic economy system using traditional Islamic financial principles. The bot manages user accounts with gold dinars and silver dirhams (displayed with the ₯ symbol), tracks zakat (Islamic charity) payments, and maintains a comprehensive transaction history. It integrates with OpenAI for enhanced functionality and uses SQLite for persistent data storage.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Bot Framework
- **Discord.py**: Core bot framework using the commands extension for structured command handling
- **Command Prefix**: Uses `!` prefix for all bot commands
- **Intents**: Configured with default intents plus message content intent for full message processing

### Data Layer
- **SQLite Database**: Local file-based database (`islamic_economy.db`) for persistent storage
- **Users Table**: Stores user profiles with gold/silver balances, zakat history, and charity totals
- **Transactions Table**: Comprehensive logging of all financial activities with transaction types and currency tracking
- **Database Initialization**: Automatic schema creation on startup

### Currency System
- **Dual Currency**: Gold dinars and silver dirhams following traditional Islamic monetary principles
- **Account Management**: Individual user accounts with balance tracking
- **Transaction Logging**: All financial activities recorded with timestamps and details

### Islamic Finance Features
- **Zakat Tracking**: Monitors last zakat payment dates for Islamic wealth purification requirements
- **Charity Management**: Tracks total charitable contributions per user
- **Transaction Categorization**: Different transaction types for various Islamic financial activities

### Asynchronous Architecture
- **Async/Await Pattern**: Non-blocking operations for database interactions and Discord API calls
- **Event-Driven**: Responds to Discord events and user commands asynchronously

## External Dependencies

### APIs and Services
- **OpenAI API**: Integration for enhanced AI-powered features (requires `OPENAI_API_KEY`)
- **Discord API**: Bot functionality through discord.py library

### Database
- **SQLite**: Embedded database for local data persistence

### Configuration
- **Environment Variables**: Secure configuration management via `.env` file
- **dotenv**: Environment variable loading for API keys and configuration

### Python Libraries
- **discord.py**: Discord bot framework
- **openai**: OpenAI API client
- **asyncio**: Asynchronous programming support
- **sqlite3**: Database connectivity (built-in)
- **datetime**: Timestamp and date management
- **typing**: Type hints for better code documentation