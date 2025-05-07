#include <Arduino.h>
#include <HardwareSerial.h>
#include <SPI.h>
#include <SD.h>
#include <SoftWire.h>

// --------------------- RTC Definitions ---------------------
#define MCP7940N_ADDRESS 0x6F
#define MFP_PIN PA1

// Create a software I2C instance for the RTC
SoftWire rtcWire(PB8, PB9, SOFT_STANDARD);

// RTC Time variables
uint8_t rtcSeconds, rtcMinutes, rtcHours, rtcDay, rtcDate, rtcMonth, rtcYear;
char rtcDateTimeStr[64];
bool rtcInitialized = false;

// RTC-based timestamps for logging
char rtcLogDate[7];  // DDMMYY format for logging
char rtcLogTime[7];  // HHMMSS format for logging

// --------------------- SD Card Pin Definitions (Using SPI2) ---------------------
#define SD_CS   PB12    // CS
#define SD_SCK  PB13    // SCK
#define SD_MISO PB14    // MISO
#define SD_MOSI PB15    // MOSI

// Create an instance of SPI2
SPIClass SPI_2(2);  // Use SPI2

// --------------------- GPS Buffer & Timezone -----------------------
char buffer[128];
int bufferIndex = 0;

// --------------------- GPS Data Struct -----------------------
struct GpsData {
  char lat[11];
  char ns;          // 'N' or 'S'
  char lon[11];
  char ew;          // 'E' or 'W'
  float speed;      // in km/h
  char mode;        // 'A' = Active, 'V' = Void
  char date[7];     // DDMMYY from GPS
  char time[7];     // HHMMSS from GPS
  bool dataReady;   // true if we got a valid RMC parse
} gpsData;

// --------------------- Keep track of last logged second ---------------------
static int lastLoggedSecond = -1;

// --------------------- Log Data Buffer for USB Transfer ---------------------
#define MAX_LOG_ENTRIES 50
char logEntries[MAX_LOG_ENTRIES][64]; // Buffer to store log entries
int logEntriesCount = 0;
bool isTransferActive = false;

// --------------------- Command Processing ---------------------
#define CMD_BUFFER_SIZE 64
char cmdBuffer[CMD_BUFFER_SIZE];
int cmdBufferIndex = 0;

// --------------------- Command Constants ---------------------
#define LOGGER_CMD "GET_72HRS_DATA"
#define CMD_HEAD "HEAD"
#define CMD_TRP "TRP"
#define CMD_1TP "1TP"
#define CMD_6TP "6TP"
#define CMD_VIOL "VIOL"
#define CMD_RST "RST"
#define SET_SPEED_PREFIX "SET_SPEED="
#define SET_LIMP_PREFIX "SET_LIMP="
#define SET_TIME_PREFIX "SET_TIME="

// --------------------- Speed Limit Settings ---------------------
int speedLimit = 80;     // Default speed limit in km/h
int limpSpeed = 40;      // Default limp speed in km/h
bool speedLimitExceeded = false;

// --------------------- Circular Queue Implementation ---------------------
// Full capacity for 72 hours of logging
#define QUEUE_CAPACITY 51840  // (72 hours * 60 minutes * 60 seconds) / 5 seconds
#define ENTRY_SIZE 64         // Fixed size of each log entry in bytes
#define BLOCK_SIZE 20         // Number of entries per batch write/read
#define INDEX_FILENAME "queue.idx"
#define DATA_FILENAME "queue.dat"

// --------------------- Queue Header Structure ---------------------
struct QueueHeader {
  uint32_t magic;             // Magic number to verify header integrity (0xGPSQ)
  uint32_t version;           // Version of the queue format
  uint32_t headIndex;         // Current head position in entries (write position)
  uint32_t tailIndex;         // Current tail position in entries (oldest data)
  uint32_t entryCount;        // Number of valid entries in the queue
  uint32_t totalEntriesEver;  // Total entries ever written (allows detecting wrapping)
  uint32_t lastTimestamp;     // Unix timestamp of the most recent entry
  uint8_t reserved[36];       // Reserved for future use (padding to 64 bytes)
};

// --------------------- Circular Queue Class ---------------------
class CircularQueue {
private:
  QueueHeader header;
  File dataFile;
  bool initialized;
  
  // Open queue files with proper access mode
  bool openFiles(bool readOnly = false) {
    if (dataFile) dataFile.close();
    
    // Make sure we're using SPI2 for SD card operations
    SPI.setModule(2);
    
    // Try multiple times to open the file
    for (int retry = 0; retry < 3; retry++) {
      if (readOnly) {
        dataFile = SD.open(DATA_FILENAME, FILE_READ);
      } else {
        dataFile = SD.open(DATA_FILENAME, FILE_WRITE);
      }
      
      if (dataFile) return true;
      delay(100); // Wait a bit before retrying
    }
    
    Serial2.print("Failed to open data file: ");
    Serial2.println(DATA_FILENAME);
    return false;
  }
  
  // Load queue header from SD card
  bool loadHeader() {
    Serial2.println("Loading queue header...");
    
    // Make sure we're using SPI2 for SD card operations
    SPI.setModule(2);
    
    File indexFile = SD.open(INDEX_FILENAME, FILE_READ);
    if (!indexFile) {
      Serial2.println("Failed to open index file for reading");
      return false;
    }
    
    size_t bytesRead = indexFile.read((uint8_t*)&header, sizeof(QueueHeader));
    indexFile.close();
    
    if (bytesRead != sizeof(QueueHeader)) {
      Serial2.print("Header read incomplete: ");
      Serial2.print(bytesRead);
      Serial2.print(" of ");
      Serial2.print(sizeof(QueueHeader));
      Serial2.println(" bytes");
      return false;
    }
    
    // Verify magic number
    if (header.magic != 0x47505351) { // 'GPSQ'
      Serial2.println("Invalid header magic number");
      return false;
    }
    
    Serial2.println("Header loaded successfully");
    return true;
  }
  
  // Save queue header to SD card
  bool saveHeader() {
    Serial2.println("Saving queue header...");
    
    // Make sure we're using SPI2 for SD card operations
    SPI.setModule(2);
    
    // Try multiple times to save the header
    for (int retry = 0; retry < 3; retry++) {
      File indexFile = SD.open(INDEX_FILENAME, FILE_WRITE);
      if (!indexFile) {
        Serial2.println("Failed to open index file for writing");
        delay(100); // Wait before retrying
        continue;
      }
      
      // Seek to beginning and write header
      indexFile.seek(0);
      size_t bytesWritten = indexFile.write((uint8_t*)&header, sizeof(QueueHeader));
      indexFile.flush(); // Ensure data is written
      indexFile.close();
      
      if (bytesWritten != sizeof(QueueHeader)) {
        Serial2.print("Header write incomplete: ");
        Serial2.print(bytesWritten);
        Serial2.print(" of ");
        Serial2.print(sizeof(QueueHeader));
        Serial2.println(" bytes");
        continue;
      }
      
      Serial2.println("Header saved successfully");
      return true;
    }
    
    Serial2.println("Failed to save header after multiple attempts");
    return false;
  }
  
  // Calculate file position from logical index
  uint32_t indexToPosition(uint32_t index) {
    return (index % QUEUE_CAPACITY) * ENTRY_SIZE;
  }

public:
  CircularQueue() : initialized(false) {
    memset(&header, 0, sizeof(QueueHeader));
    header.magic = 0x47505351; // 'GPSQ'
    header.version = 1;
  }
  
  // Check if queue is initialized
  bool isInitialized() const {
    return initialized;
  }
  
  // Initialize the circular queue
  bool begin() {
    Serial2.println("Starting CircularQueue initialization...");
    
    // Try to load existing header
    if (SD.exists(INDEX_FILENAME)) {
      Serial2.println("Index file exists, attempting to load");
      if (loadHeader()) {
        Serial2.println("Circular queue loaded from SD");
        initialized = true;
        return true;
      } else {
        Serial2.println("Failed to load existing header, creating new");
        // If the file exists but is corrupt, remove it to start fresh
        SD.remove(INDEX_FILENAME);
      }
    } else {
      Serial2.println("Index file doesn't exist, creating new");
    }
    
    // Create new queue if loading failed
    header.headIndex = 0;
    header.tailIndex = 0;
    header.entryCount = 0;
    header.totalEntriesEver = 0;
    header.lastTimestamp = 0;
    
    // First check if we can create/open the index file
    {
      File testIdx = SD.open(INDEX_FILENAME, FILE_WRITE);
      if (!testIdx) {
        Serial2.println("Cannot create index file - check SD card permissions");
        return false;
      }
      testIdx.close();
    }
    
    // Create the data file if needed
    if (!SD.exists(DATA_FILENAME)) {
      Serial2.println("Data file doesn't exist, creating it");
      dataFile = SD.open(DATA_FILENAME, FILE_WRITE);
      if (!dataFile) {
        Serial2.println("Failed to create data file - check SD card");
        return false;
      }
      
      // Instead of pre-allocating the entire file, just write a small test
      Serial2.println("Testing data file write...");
      char testData[64] = "CircularQueueTestData";
      if (dataFile.write((uint8_t*)testData, sizeof(testData)) != sizeof(testData)) {
        Serial2.println("Failed to write test data to data file");
        dataFile.close();
        return false;
      }
      
      dataFile.close();
      Serial2.println("Data file created successfully");
    } else {
      Serial2.println("Data file already exists");
    }
    
    // Save the new header
    if (!saveHeader()) {
      Serial2.println("Failed to save header - queue initialization aborted");
      return false;
    }
    
    Serial2.println("Circular queue initialized successfully");
    initialized = true;
    return true;
  }
  
  // Add a new entry to the queue
  bool enqueue(const char* entry, uint32_t timestamp) {
    if (!initialized) {
      Serial2.println("Queue not initialized - cannot enqueue");
      return false;
    }
    
    // Open data file for writing
    if (!openFiles()) {
      Serial2.println("Failed to open data file for writing");
      return false;
    }
    
    // Seek to write position
    uint32_t position = indexToPosition(header.headIndex);
    if (!dataFile.seek(position)) {
      Serial2.println("Failed to seek to write position");
      dataFile.close();
      return false;
    }
    
    // Write data
    size_t entryLen = strlen(entry);
    size_t bytesWritten = dataFile.write((uint8_t*)entry, entryLen);
    
    // Check if write was successful
    if (bytesWritten != entryLen) {
      Serial2.print("Write incomplete: ");
      Serial2.print(bytesWritten);
      Serial2.print(" of ");
      Serial2.print(entryLen);
      Serial2.println(" bytes");
      dataFile.close();
      return false;
    }
    
    // Fill remaining bytes with zeros
    if (bytesWritten < ENTRY_SIZE) {
      uint8_t zeros[ENTRY_SIZE - bytesWritten];
      memset(zeros, 0, ENTRY_SIZE - bytesWritten);
      dataFile.write(zeros, ENTRY_SIZE - bytesWritten);
    }
    
    // Update header
    header.headIndex = (header.headIndex + 1) % QUEUE_CAPACITY;
    header.totalEntriesEver++;
    header.lastTimestamp = timestamp;
    
    // If queue is full, move tail
    if (header.entryCount >= QUEUE_CAPACITY) {
      header.tailIndex = (header.tailIndex + 1) % QUEUE_CAPACITY;
    } else {
      header.entryCount++;
    }
    
    dataFile.close();
    
    // Save updated header
    if (!saveHeader()) {
      Serial2.println("Warning: Failed to update header after write");
      // Continue anyway since the data was written
    }
    
    return true;
  }
  
  // Read entries from queue for a specified time range
  bool readEntriesSince(uint32_t sinceTimestamp, void (*callback)(const char* entry)) {
    if (!initialized || header.entryCount == 0) {
      Serial2.println("Queue not initialized or empty");
      return false;
    }
    
    // Open data file for reading
    if (!openFiles(true)) {
      Serial2.println("Failed to open data file for reading");
      return false;
    }
    
    // Determine how many entries to read
    uint32_t entriesToRead = header.entryCount;
    uint32_t currentIndex = header.tailIndex;
    
    char entryBuffer[ENTRY_SIZE + 1]; // +1 for null terminator
    int entriesSent = 0;
    
    for (uint32_t i = 0; i < entriesToRead; i++) {
      uint32_t position = indexToPosition(currentIndex);
      dataFile.seek(position);
      
      // Read entry
      memset(entryBuffer, 0, ENTRY_SIZE + 1);
      size_t bytesRead = dataFile.read((uint8_t*)entryBuffer, ENTRY_SIZE);
      if (bytesRead != ENTRY_SIZE) {
        Serial2.println("Warning: Incomplete read from queue");
      }
      
      entryBuffer[ENTRY_SIZE] = '\0'; // Ensure null termination
      
      // Process entry (extract timestamp from DDMMYY,HHMMSS format)
      if (strlen(entryBuffer) > 13) { // Minimum length for a valid entry
        int day = (entryBuffer[0] - '0') * 10 + (entryBuffer[1] - '0');
        int month = (entryBuffer[2] - '0') * 10 + (entryBuffer[3] - '0');
        int year = 2000 + (entryBuffer[4] - '0') * 10 + (entryBuffer[5] - '0');
        
        int hour = (entryBuffer[7] - '0') * 10 + (entryBuffer[8] - '0');
        int minute = (entryBuffer[9] - '0') * 10 + (entryBuffer[10] - '0');
        int second = (entryBuffer[11] - '0') * 10 + (entryBuffer[12] - '0');
        
        // Simple timestamp calculation
        uint32_t entryTimestamp = ((year-1970)*365 + month*30 + day) * 86400 + hour*3600 + minute*60 + second;
        
        // Check if this entry is recent enough
        if (entryTimestamp >= sinceTimestamp) {
          callback(entryBuffer);
          entriesSent++;
          
          // Add small delay to prevent buffer overflow
          if (entriesSent % 10 == 0) {
            delay(50);
          }
        }
      }
      
      // Move to next entry
      currentIndex = (currentIndex + 1) % QUEUE_CAPACITY;
    }
    
    dataFile.close();
    return entriesSent > 0;
  }
  
  // Get queue statistics
  void getStats(uint32_t* count, uint32_t* capacity, uint32_t* latestTimestamp) {
    if (count) *count = header.entryCount;
    if (capacity) *capacity = QUEUE_CAPACITY;
    if (latestTimestamp) *latestTimestamp = header.lastTimestamp;
  }
};

// Create global circular queue instance
CircularQueue gpsQueue;

// Callback function to process and send entries during retrieval
void processQueueEntry(const char* entry) {
  // Send entry over Serial
  Serial.println(entry);
}

// --------------------- RTC Functions ---------------------
bool mcp7940n_write_register(uint8_t reg, uint8_t value) {
  rtcWire.beginTransmission(MCP7940N_ADDRESS);
  rtcWire.write(reg);
  rtcWire.write(value);
  return rtcWire.endTransmission() == 0;
}

bool mcp7940n_read_register(uint8_t reg, uint8_t *value) {
  rtcWire.beginTransmission(MCP7940N_ADDRESS);
  rtcWire.write(reg);
  if (rtcWire.endTransmission(false) != 0) return false;
  if (rtcWire.requestFrom(MCP7940N_ADDRESS, 1) != 1) return false;
  *value = rtcWire.read();
  return true;
}

bool mcp7940n_read_time() {
  uint8_t value;
  if (!mcp7940n_read_register(0x00, &value)) return false;
  rtcSeconds = (value & 0x0F) + ((value & 0x70) >> 4) * 10;

  if (!mcp7940n_read_register(0x01, &value)) return false;
  rtcMinutes = (value & 0x0F) + ((value & 0x70) >> 4) * 10;

  if (!mcp7940n_read_register(0x02, &value)) return false;
  rtcHours = (value & 0x0F) + ((value & 0x30) >> 4) * 10;

  if (!mcp7940n_read_register(0x03, &value)) return false;
  rtcDay = value & 0x07;

  if (!mcp7940n_read_register(0x04, &value)) return false;
  rtcDate = (value & 0x0F) + ((value & 0x30) >> 4) * 10;

  if (!mcp7940n_read_register(0x05, &value)) return false;
  rtcMonth = (value & 0x0F) + ((value & 0x10) >> 4) * 10;

  if (!mcp7940n_read_register(0x06, &value)) return false;
  rtcYear = (value & 0x0F) + ((value & 0xF0) >> 4) * 10;

  return true;
}

bool mcp7940n_set_time(uint8_t y, uint8_t m, uint8_t d, uint8_t h, uint8_t min, uint8_t sec) {
  uint8_t sec_bcd = ((sec / 10) << 4) | (sec % 10);
  uint8_t min_bcd = ((min / 10) << 4) | (min % 10);
  uint8_t hour_bcd = ((h / 10) << 4) | (h % 10);
  uint8_t date_bcd = ((d / 10) << 4) | (d % 10);
  uint8_t month_bcd = ((m / 10) << 4) | (m % 10);
  uint8_t year_bcd = ((y / 10) << 4) | (y % 10);
  uint8_t wkday = 1; // Monday (placeholder - would be calculated in full implementation)

  sec_bcd |= 0x80; // Start oscillator

  return
    mcp7940n_write_register(0x00, sec_bcd) &&
    mcp7940n_write_register(0x01, min_bcd) &&
    mcp7940n_write_register(0x02, hour_bcd) &&
    mcp7940n_write_register(0x03, wkday | 0x08) &&
    mcp7940n_write_register(0x04, date_bcd) &&
    mcp7940n_write_register(0x05, month_bcd) &&
    mcp7940n_write_register(0x06, year_bcd);
}

// Initialize the RTC module
bool mcp7940n_init() {
  rtcWire.begin();
  uint8_t dummy;
  return mcp7940n_read_register(0x03, &dummy);
}

// --------------------- Helper Functions ---------------------
// Calculate timestamp from RTC values
uint32_t calculateRtcTimestamp() {
  // Simple timestamp calculation using RTC values
  return ((2000+rtcYear-1970)*365 + rtcMonth*30 + rtcDate) * 86400 + 
         rtcHours*3600 + rtcMinutes*60 + rtcSeconds;
}

// --------------------- Parse RMC Sentence ---------------------
void parseRMC(char* sentence) {
  // Example: $GPRMC,HHMMSS,A,lat,NS,lon,EW,speed,angle,date,...
  // or       $GNRMC,HHMMSS,A,lat,NS,lon,EW,speed,angle,date,...
  char* token = strtok(sentence, ",");
  int fieldCount = 0;
  
  // Reset dataReady each new parse
  gpsData.dataReady = false;

  while (token != NULL) {
    switch(fieldCount) {
      case 1: // Time - just store it, but we won't use it for timestamps
        if (strlen(token) >= 6) {
          strncpy(gpsData.time, token, 6);
          gpsData.time[6] = '\0';
        }
        break;
      case 2: // Status (A=Active, V=Void)
        if (strlen(token) > 0) {
          gpsData.mode = token[0];
        }
        break;
      case 3: // Latitude
        if (strlen(token) > 0) {
          strncpy(gpsData.lat, token, 10);
          gpsData.lat[10] = '\0';
        }
        break;
      case 4: // N/S
        if (strlen(token) > 0) {
          gpsData.ns = token[0];
        }
        break;
      case 5: // Longitude
        if (strlen(token) > 0) {
          strncpy(gpsData.lon, token, 10);
          gpsData.lon[10] = '\0';
        }
        break;
      case 6: // E/W
        if (strlen(token) > 0) {
          gpsData.ew = token[0];
        }
        break;
      case 7: // Speed in knots
        if (strlen(token) > 0) {
          gpsData.speed = atof(token) * 1.852; // knots -> km/h
          
          // Check speed limit
          if (gpsData.speed > speedLimit) {
            speedLimitExceeded = true;
          } else {
            speedLimitExceeded = false;
          }
        } else {
          gpsData.speed = 0.0;
        }
        break;
      case 9: // Date - just store it, but we won't use it for timestamps
        if (strlen(token) >= 6) {
          strncpy(gpsData.date, token, 6);
          gpsData.date[6] = '\0';
          gpsData.dataReady = true;
          
          // Optionally sync RTC with GPS time (once only) if needed
          // This is commented out since user wants to always use RTC time
          // static bool rtcSyncedWithGps = false;
          // if (!rtcSyncedWithGps && gpsData.mode == 'A') {
          //   updateRtcFromGps();
          //   rtcSyncedWithGps = true;
          // }
        }
        break;
    }
    token = strtok(NULL, ",");
    fieldCount++;
  }

  // Get the current RTC time for logging (ALWAYS using RTC time)
  if (!mcp7940n_read_time()) {
    Serial2.println("Warning: Failed to read RTC time");
    return; // Skip logging if RTC read fails
  }
  
  // Format RTC time for logging
  sprintf(rtcLogDate, "%02d%02d%02d", rtcDate, rtcMonth, rtcYear);
  sprintf(rtcLogTime, "%02d%02d%02d", rtcHours, rtcMinutes, rtcSeconds);
  
  // --------------------- EXACT 5s LOGGING (RTC Time Based) ---------------------
  // Extract seconds from RTC time
  int currentSec = rtcSeconds;
  
  // If it's exactly a multiple of 5 seconds
  if ((currentSec % 5) == 0) {
    // And we haven't logged this particular second yet
    if (currentSec != lastLoggedSecond) {
      lastLoggedSecond = currentSec;
      
      // Print GPS + RTC data for debugging
      Serial2.print("RTC: ");
      Serial2.print(rtcLogDate);
      Serial2.print(",");
      Serial2.print(rtcLogTime);
      
      if (gpsData.dataReady) {
        Serial2.print(" | GPS: ");
        Serial2.print(gpsData.lon);
        Serial2.print(gpsData.ew);
        Serial2.print(",");
        Serial2.print(gpsData.lat);
        Serial2.print(gpsData.ns);
        Serial2.print(",");
        Serial2.println(gpsData.speed, 1);
      } else {
        Serial2.println(" | No valid GPS position");
      }

      // Prepare log line
      char logLine[64];
      
      if (gpsData.dataReady && gpsData.mode == 'A') {
        // GPS data is valid, include position
        char speedStr[10];  // Buffer to hold the converted float as a string
        dtostrf(gpsData.speed, 4, 1, speedStr); // Convert float to string: 4 digits, 1 decimal

        sprintf(logLine, "%s,%s,%s%c,%s%c,%s", 
                rtcLogDate, rtcLogTime,  // Using RTC time for timestamp
                gpsData.lon, gpsData.ew, 
                gpsData.lat, gpsData.ns, 
                speedStr);
      } else {
        // GPS data is invalid or signal lost
        sprintf(logLine, "%s,%s,SL,SL,SL", rtcLogDate, rtcLogTime);  // Using RTC time
      }
      
      // Only log to queue if it's initialized
      if (gpsQueue.isInitialized()) {
        // Calculate timestamp for queue from RTC time
        uint32_t timestamp = ((2000+rtcYear-1970)*365 + rtcMonth*30 + rtcDate) * 86400 + 
                             rtcHours*3600 + rtcMinutes*60 + rtcSeconds;
        
        // Log to circular queue for 72-hour data
        bool logSuccess = gpsQueue.enqueue(logLine, timestamp);

        // Print status of logging operation
        if (logSuccess) {
          Serial2.println("Queue write successful");
        } else {
          Serial2.println("Queue write failed!");
        }
      } else {
        Serial2.println("Queue not initialized - skipping queue log");
      }

      // Store in memory buffer for USB transfer regardless of queue status
      if (logEntriesCount < MAX_LOG_ENTRIES) {
        strcpy(logEntries[logEntriesCount], logLine);
        logEntriesCount++;
      }

      // Print to Serial2 for debug
      Serial2.println("Data logged. Logged line:");
      Serial2.println(logLine);
    }
  }
}

// --------------------- Process Commands from USB ---------------------
void processCommand(char* cmd) {
  // Remove any newline or carriage return
  char* newline = strchr(cmd, '\n');
  if (newline) *newline = '\0';
  
  newline = strchr(cmd, '\r');
  if (newline) *newline = '\0';
  
  // Process the commands
  if (strcmp(cmd, LOGGER_CMD) == 0) {
    // Get data from circular queue for last 72 hours
    Serial.println("Retrieving data from circular queue for last 72 hours...");
    
    // Only proceed if queue is initialized
    if (!gpsQueue.isInitialized()) {
      Serial.println("ERROR: Circular queue not initialized");
      return;
    }
    
    // Always use current RTC time
    uint32_t currentTimestamp = 0;
    if (rtcInitialized && mcp7940n_read_time()) {
      currentTimestamp = calculateRtcTimestamp();
    } else {
      // Fallback to using queue's latest timestamp
      uint32_t latestTimestamp;
      gpsQueue.getStats(NULL, NULL, &latestTimestamp);
      currentTimestamp = latestTimestamp > 0 ? latestTimestamp : 0;
    }
    
    // Calculate timestamp for 72 hours ago
    uint32_t secondsIn72Hours = 3 * 24 * 60 * 60;
    uint32_t sinceTimestamp = currentTimestamp > secondsIn72Hours ? 
                              currentTimestamp - secondsIn72Hours : 0;
    
    // Print debug info
    Serial.print("Current timestamp (from RTC): ");
    Serial.println(currentTimestamp);
    Serial.print("Cutoff timestamp (72 hours ago): ");
    Serial.println(sinceTimestamp);
    
    // Get queue stats
    uint32_t count, capacity, latestTimestamp;
    gpsQueue.getStats(&count, &capacity, &latestTimestamp);
    Serial.print("Queue capacity: "); Serial.println(capacity);
    Serial.print("Entries in queue: "); Serial.println(count);
    
    // Read and process entries since the cutoff time
    if (gpsQueue.readEntriesSince(sinceTimestamp, processQueueEntry)) {
      Serial.println("Transfer complete");
    } else {
      Serial.println("No entries found or transfer failed");
    }
  } 
  else if (strcmp(cmd, CMD_HEAD) == 0) {
    Serial.println("Current settings:");
    Serial.print("Speed limit: "); Serial.println(speedLimit);
    Serial.print("Limp speed: "); Serial.println(limpSpeed);
    
    // Only show RTC time (not using GPS time)
    if (rtcInitialized && mcp7940n_read_time()) {
      Serial.print("RTC time: ");
      Serial.print("20"); Serial.print(rtcYear); Serial.print("/");
      Serial.print(rtcMonth); Serial.print("/");
      Serial.print(rtcDate); Serial.print(" ");
      Serial.print(rtcHours); Serial.print(":");
      Serial.print(rtcMinutes); Serial.print(":");
      Serial.println(rtcSeconds);
    } else {
      Serial.println("RTC time not available");
    }
    
    // Show GPS status if available
    if (gpsData.dataReady) {
      Serial.print("GPS Status: ");
      if (gpsData.mode == 'A') {
        Serial.println("Valid fix");
        Serial.print("Coordinates: ");
        Serial.print(gpsData.lon); Serial.print(gpsData.ew); Serial.print(", ");
        Serial.print(gpsData.lat); Serial.println(gpsData.ns);
        Serial.print("Speed: "); Serial.print(gpsData.speed, 1); Serial.println(" km/h");
      } else {
        Serial.println("No fix");
      }
    } else {
      Serial.println("GPS data not available");
    }
    
    // Add circular queue info
    if (gpsQueue.isInitialized()) {
      uint32_t count, capacity, latestTimestamp;
      gpsQueue.getStats(&count, &capacity, &latestTimestamp);
      Serial.print("Queue entries: "); Serial.print(count); 
      Serial.print(" of "); Serial.print(capacity);
      Serial.print(" ("); Serial.print((count * 100) / capacity); 
      Serial.println("%)");
    } else {
      Serial.println("Circular queue not initialized");
    }
  } 
  else if (strcmp(cmd, CMD_TRP) == 0) {
    Serial.println("Trip data:");
    Serial.println("TripID,StartTime,EndTime,Distance,MaxSpeed,AvgSpeed");
    Serial.println("1,230145,231532,12.5,78.5,45.2");
    // This is a placeholder - you'd implement actual trip data tracking
  } 
  else if (strcmp(cmd, CMD_1TP) == 0) {
    Serial.println("Last trip data:");
    Serial.println("StartTime,EndTime,Distance,MaxSpeed,AvgSpeed");
    Serial.println("230145,231532,12.5,78.5,45.2");
    // This is a placeholder - you'd implement actual trip data tracking
  } 
  else if (strcmp(cmd, CMD_6TP) == 0) {
    Serial.println("Last 6 trips data:");
    Serial.println("TripID,StartTime,EndTime,Distance,MaxSpeed,AvgSpeed");
    Serial.println("1,230145,231532,12.5,78.5,45.2");
    Serial.println("2,232145,233532,8.3,65.2,38.7");
    // This is a placeholder - you'd add more trip data
  } 
  else if (strcmp(cmd, CMD_VIOL) == 0) {
    Serial.println("Speed violations:");
    Serial.println("Time,Speed,Duration");
    Serial.println("231245,85.2,45");
    // This is a placeholder - you'd implement actual violation tracking
  } 
  else if (strcmp(cmd, CMD_RST) == 0) {
    Serial.println("Resetting device...");
    // Reset relevant counters and states
    logEntriesCount = 0;
    speedLimitExceeded = false;
    Serial.println("Reset complete");
  }
  else if (strncmp(cmd, SET_SPEED_PREFIX, strlen(SET_SPEED_PREFIX)) == 0) {
    // Extract the speed value
    int newSpeed = atoi(cmd + strlen(SET_SPEED_PREFIX));
    if (newSpeed > 0 && newSpeed < 200) {
      speedLimit = newSpeed;
      Serial.print("Speed limit set to: ");
      Serial.println(speedLimit);
    } else {
      Serial.println("Invalid speed value");
    }
  }
  else if (strncmp(cmd, SET_LIMP_PREFIX, strlen(SET_LIMP_PREFIX)) == 0) {
    // Extract the limp speed value
    int newLimpSpeed = atoi(cmd + strlen(SET_LIMP_PREFIX));
    if (newLimpSpeed > 0 && newLimpSpeed < speedLimit) {
      limpSpeed = newLimpSpeed;
      Serial.print("Limp speed set to: ");
      Serial.println(limpSpeed);
    } else {
      Serial.println("Invalid limp speed value");
    }
  }
  else if (strncmp(cmd, SET_TIME_PREFIX, strlen(SET_TIME_PREFIX)) == 0) {
    // This command sets only the RTC time now
    char* timeStr = cmd + strlen(SET_TIME_PREFIX);
    // Format expected: "YY-MM-DD-HH-MM-SS"
    if (strlen(timeStr) >= 17) {  // Check if proper length
      int y = (timeStr[0] - '0') * 10 + (timeStr[1] - '0');
      int m = (timeStr[3] - '0') * 10 + (timeStr[4] - '0');
      int d = (timeStr[6] - '0') * 10 + (timeStr[7] - '0');
      int h = (timeStr[9] - '0') * 10 + (timeStr[10] - '0');
      int min = (timeStr[12] - '0') * 10 + (timeStr[13] - '0');
      int sec = (timeStr[15] - '0') * 10 + (timeStr[16] - '0');
      
      if (rtcInitialized && mcp7940n_set_time(y, m, d, h, min, sec)) {
        Serial.println("RTC time set successfully");
        Serial.print("New RTC time: 20");
        Serial.print(y); Serial.print("-");
        Serial.print(m); Serial.print("-");
        Serial.print(d); Serial.print(" ");
        Serial.print(h); Serial.print(":");
        Serial.print(min); Serial.print(":");
        Serial.println(sec);
      } else {
        Serial.println("Failed to set RTC time");
      }
    } else {
      Serial.println("Invalid time format. Use: YY-MM-DD-HH-MM-SS");
    }
  }
  else {
    Serial.println("Unknown command");
  }
}

// --------------------- Setup ---------------------
void setup() {
  // Start Serial (USB) at the baud rate expected by the PC application
  Serial.begin(115200);
  delay(1500); // Give USB time to initialize
  
  // Start Serial2 for GPS communication and debugging
  Serial2.begin(9600);
  delay(1000);

  Serial2.println("\nGPS Logger with RTC (PRIMARY TIME SOURCE), Circular Queue and USB Communication");
  Serial.println("\nSTM32 GPS Logger with RTC (PRIMARY TIME SOURCE) and 72-Hour Circular Queue");
  Serial.println("Ready to receive commands.");

  // Initialize RTC - THIS IS THE PRIMARY TIME SOURCE
  Serial2.println("Initializing RTC (primary time source)...");
  rtcWire.begin();
  delay(100);  // Give I2C time to stabilize
  
  if (mcp7940n_init()) {
    Serial2.println("RTC initialized successfully");
    rtcInitialized = true;
    
    // Read current time from RTC
    if (mcp7940n_read_time()) {
      sprintf(rtcDateTimeStr, "20%02d/%02d/%02d %02d:%02d:%02d",
              rtcYear, rtcMonth, rtcDate, rtcHours, rtcMinutes, rtcSeconds);
      Serial2.print("RTC time: ");
      Serial2.println(rtcDateTimeStr);
      
      // Format RTC time for logging
      sprintf(rtcLogDate, "%02d%02d%02d", rtcDate, rtcMonth, rtcYear);
      sprintf(rtcLogTime, "%02d%02d%02d", rtcHours, rtcMinutes, rtcSeconds);
    } else {
      Serial2.println("Failed to read time from RTC");
      // Set default RTC time if it wasn't set before
      if (mcp7940n_set_time(25, 5, 7, 12, 0, 0)) {
        Serial2.println("Set default RTC time: 2025-05-07 12:00:00");
        
        // Read the time we just set
        if (mcp7940n_read_time()) {
          // Format RTC time for logging
          sprintf(rtcLogDate, "%02d%02d%02d", rtcDate, rtcMonth, rtcYear);
          sprintf(rtcLogTime, "%02d%02d%02d", rtcHours, rtcMinutes, rtcSeconds);
        }
      }
    }
  } else {
    Serial2.println("Failed to initialize RTC - CRITICAL ERROR!");
    Serial2.println("System will not have accurate time source.");
  }

  // Configure SPI pins for SD card
  pinMode(SD_CS, OUTPUT);
  digitalWrite(SD_CS, HIGH); // Deselect SD card

  // Initialize SPI2 with conservative settings
  SPI_2.begin();
  SPI_2.setClockDivider(SPI_CLOCK_DIV128); // Very slow speed for reliability
  SPI_2.setDataMode(SPI_MODE0);
  SPI_2.setBitOrder(MSBFIRST);

  Serial2.println("\nPower-up sequence for SD card on SPI2:");
  Serial2.println("Using SPI2 configuration:");
  Serial2.println("CS   -> PB12");
  Serial2.println("SCK  -> PB13");
  Serial2.println("MISO -> PB14");
  Serial2.println("MOSI -> PB15");
  
  digitalWrite(SD_CS, HIGH);
  delay(100);

  // Send dummy clock cycles with CS high
  for(int i = 0; i < 10; i++) {
    SPI_2.transfer(0xFF);
    Serial2.print(".");
  }
  Serial2.println(" Done");

  delay(100);

  Serial2.println("\nInitializing SD card on SPI2...");
  
  // Select SPI2 for SD card operations
  SPI.setModule(2);
  
  // Attempt to initialize SD with more careful error checking
  for (int retry = 0; retry < 3; retry++) {
    if (SD.begin(SD_CS)) {
      Serial2.println("SD card initialization successful!");
      
      // Test file operations
      File dataFile = SD.open("test.txt", FILE_WRITE);
      if (dataFile) {
        Serial2.println("\nWriting to test.txt...");
        dataFile.println("Testing SD card with level shifter");
        dataFile.println("Module is working properly!");
        dataFile.close();
        Serial2.println("Write successful!");
        
        // Verify we can read the file back
        dataFile = SD.open("test.txt", FILE_READ);
        if (dataFile) {
          Serial2.println("Reading from test.txt:");
          while (dataFile.available()) {
            Serial2.write(dataFile.read());
          }
          dataFile.close();
          Serial2.println("\nRead test successful!");
        } else {
          Serial2.println("Failed to open test.txt for reading!");
          Serial2.println("SD card may be write-protected or corrupted");
          // Continue without SD card
          break;
        }
      } else {
        Serial2.println("Failed to open test.txt for writing!");
        Serial2.println("SD card may be write-protected or corrupted");
        Serial2.println("Check the physical write protection switch on the SD card");
        // Continue without SD card
        break;
      }
      
      // Initialize circular queue with better error reporting
      Serial2.println("\nInitializing circular queue...");
      if (gpsQueue.begin()) {
        Serial2.println("Circular queue initialized successfully");
        uint32_t count, capacity, timestamp;
        gpsQueue.getStats(&count, &capacity, &timestamp);
        Serial2.print("Queue capacity: "); Serial2.println(capacity);
        Serial2.print("Entries in queue: "); Serial2.println(count);
      } else {
        Serial2.println("Failed to initialize circular queue! Logger will continue without queue functionality.");
      }
      
      // SD card initialized and tested successfully
      break;
    } else {
      Serial2.print("SD card initialization failed! Retry ");
      Serial2.print(retry + 1);
      Serial2.println("/3");
      delay(500);
    }
  }

  // Clear GPS data struct
  memset(&gpsData, 0, sizeof(gpsData));
  gpsData.mode = 'V'; // Initial mode is Void until we get a valid fix
  
  // Initialize GPS data fields with default values
  strcpy(gpsData.lat, "0000.0000");
  gpsData.ns = 'N';
  strcpy(gpsData.lon, "00000.0000");
  gpsData.ew = 'E';
  gpsData.speed = 0.0;
  strcpy(gpsData.date, "010120"); // Default date (01-01-2020)
  strcpy(gpsData.time, "000000"); // Default time (00:00:00)
  gpsData.dataReady = false;
  
  // Initialize command buffer
  memset(cmdBuffer, 0, CMD_BUFFER_SIZE);
  cmdBufferIndex = 0;
}

// --------------------- Main Loop ---------------------
void loop() {
  // 1. Check for USB commands
  while (Serial.available() > 0) {
    char c = Serial.read();
    
    // Echo character back to USB
    Serial.write(c);
    
    // Add to command buffer if not end of line
    if (c != '\n' && c != '\r') {
      if (cmdBufferIndex < CMD_BUFFER_SIZE - 1) {
        cmdBuffer[cmdBufferIndex++] = c;
      }
    } else {
      // Process command on newline or carriage return
      if (cmdBufferIndex > 0) {
        cmdBuffer[cmdBufferIndex] = '\0';
        processCommand(cmdBuffer);
        cmdBufferIndex = 0;
      }
    }
  }

  // 2. Process GPS data
  while (Serial2.available() > 0) {
    char c = Serial2.read();

    if (c == '$') { // Start of NMEA
      bufferIndex = 0;
    }
    else if (c == '\n' || c == '\r') { // End of NMEA
      if (bufferIndex > 0) {
        buffer[bufferIndex] = '\0';
        // Check if it's RMC
        if (strstr(buffer, "GNRMC") || strstr(buffer, "GPRMC")) {
          parseRMC(buffer);
        }
      }
      bufferIndex = 0;
    }
    else if (bufferIndex < (int)sizeof(buffer) - 1) {
      buffer[bufferIndex++] = c;
    }
  }

  // 3. Force RTC-based logging even if no GPS data received
  static unsigned long lastForcedLogCheck = 0;
  if (millis() - lastForcedLogCheck >= 1000) { // Check every second
    lastForcedLogCheck = millis();
    
    // Get current RTC time
    if (rtcInitialized && mcp7940n_read_time()) {
      // Check if we need to log (every 5 seconds)
      if ((rtcSeconds % 5) == 0 && rtcSeconds != lastLoggedSecond) {
        lastLoggedSecond = rtcSeconds;
        
        // Format RTC time for logging
        sprintf(rtcLogDate, "%02d%02d%02d", rtcDate, rtcMonth, rtcYear);
        sprintf(rtcLogTime, "%02d%02d%02d", rtcHours, rtcMinutes, rtcSeconds);
        
        // Create log line
        char logLine[64];
        
        if (gpsData.dataReady && gpsData.mode == 'A') {
          // Valid GPS position available
          char speedStr[10];
          dtostrf(gpsData.speed, 4, 1, speedStr);
          
          sprintf(logLine, "%s,%s,%s%c,%s%c,%s", 
                  rtcLogDate, rtcLogTime,
                  gpsData.lon, gpsData.ew, 
                  gpsData.lat, gpsData.ns, 
                  speedStr);
        } else {
          // No valid GPS position
          sprintf(logLine, "%s,%s,SL,SL,SL", rtcLogDate, rtcLogTime);
        }
        
        // Log to queue
        if (gpsQueue.isInitialized()) {
          uint32_t timestamp = calculateRtcTimestamp();
          bool logSuccess = gpsQueue.enqueue(logLine, timestamp);
          
          if (logSuccess) {
            Serial2.println("Queue write successful");
          } else {
            Serial2.println("Queue write failed!");
          }
        }
        
        // Store in memory buffer
        if (logEntriesCount < MAX_LOG_ENTRIES) {
          strcpy(logEntries[logEntriesCount], logLine);
          logEntriesCount++;
        }
        
        // Debug output
        Serial2.println("Data logged (RTC-based). Logged line:");
        Serial2.println(logLine);
      }
      
      // Display RTC time for debug purposes
      static unsigned long lastDisplayTime = 0;
      if (millis() - lastDisplayTime >= 1000) {
        lastDisplayTime = millis();
        
        sprintf(rtcDateTimeStr, "20%02d/%02d/%02d %02d:%02d:%02d",
                rtcYear, rtcMonth, rtcDate, rtcHours, rtcMinutes, rtcSeconds);
        Serial2.print("RTC: ");
        Serial2.println(rtcDateTimeStr);
        
        // Show GPS status if available
        if (gpsData.dataReady) {
          Serial2.print("GPS: ");
          if (gpsData.mode == 'A') {
            Serial2.print("Valid fix, ");
            Serial2.print(gpsData.lon); Serial2.print(gpsData.ew); Serial2.print(", ");
            Serial2.print(gpsData.lat); Serial2.print(gpsData.ns); Serial2.print(", ");
            Serial2.print(gpsData.speed, 1); Serial2.println(" km/h");
          } else {
            Serial2.println("No fix");
          }
        }
      }
    }
  }
}
