"""
F Prime Protocol Implementation

Implements the F Prime communications protocol as specified in:
https://fprime.jpl.nasa.gov/latest/Svc/FprimeProtocol/docs/sdd/

Frame Format:
- Start word: 32-bit, always 0xDEADBEEF (big-endian)
- Payload length: 32-bit, specifies payload length in bytes (big-endian)
- Payload data: Variable-length F Prime packet
- CRC: 32-bit CRC32 for integrity verification
"""
import struct
import zlib
import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# F Prime protocol constants
FPRIME_START_WORD = 0xDEADBEEF
FPRIME_START_WORD_BYTES = struct.pack('>I', FPRIME_START_WORD)
FRAME_HEADER_SIZE = 8  # Start word (4) + Payload length (4)
FRAME_FOOTER_SIZE = 4  # CRC (4)
MIN_FRAME_SIZE = FRAME_HEADER_SIZE + FRAME_FOOTER_SIZE  # 12 bytes minimum


class FPrimeFrameError(Exception):
    """Exception raised for F Prime frame parsing errors.

    This exception is raised when frame parsing fails due to invalid format,
    missing data, or CRC mismatch.
    """
    pass


class FPrimeFrame:
    """Represents a complete F Prime frame.

    A frame consists of a start word (0xDEADBEEF), payload length, payload data,
    and CRC32 checksum. This class handles serialization, deserialization, and
    CRC verification.
    """
    
    def __init__(self, payload: bytes, crc: Optional[int] = None):
        """Initialize an F Prime frame.

        Args:
            payload: The payload data (F Prime packet).
            crc: Optional CRC value. If None, will be calculated automatically.
        """
        self.payload = payload
        self.crc = crc if crc is not None else self._calculate_crc()
    
    def _calculate_crc(self) -> int:
        """Calculate CRC32 for the frame.

        CRC covers: start word + payload length + payload.

        Returns:
            32-bit CRC32 value as unsigned integer.
        """
        # CRC covers: start word + payload length + payload
        data = FPRIME_START_WORD_BYTES
        data += struct.pack('>I', len(self.payload))
        data += self.payload
        return zlib.crc32(data) & 0xFFFFFFFF
    
    def to_bytes(self) -> bytes:
        """Serialize frame to bytes according to F Prime protocol.

        Returns:
            Complete frame as bytes: start word + payload length + payload + CRC.
        """
        frame = FPRIME_START_WORD_BYTES
        frame += struct.pack('>I', len(self.payload))
        frame += self.payload
        frame += struct.pack('>I', self.crc)
        return frame
    
    def verify_crc(self) -> bool:
        """Verify the CRC of this frame.

        Returns:
            True if CRC matches calculated value, False otherwise.
        """
        expected_crc = self._calculate_crc()
        return self.crc == expected_crc
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'FPrimeFrame':
        """
        Parse an F Prime frame from bytes.
        
        Args:
            data: Complete frame bytes
            
        Returns:
            FPrimeFrame instance
            
        Raises:
            FPrimeFrameError: If frame is invalid
        """
        if len(data) < MIN_FRAME_SIZE:
            raise FPrimeFrameError(f"Frame too short: {len(data)} bytes (minimum {MIN_FRAME_SIZE})")
        
        # Parse start word
        start_word = struct.unpack('>I', data[0:4])[0]
        if start_word != FPRIME_START_WORD:
            raise FPrimeFrameError(f"Invalid start word: 0x{start_word:08X} (expected 0x{FPRIME_START_WORD:08X})")
        
        # Parse payload length
        payload_length = struct.unpack('>I', data[4:8])[0]
        
        # Validate frame size
        expected_frame_size = FRAME_HEADER_SIZE + payload_length + FRAME_FOOTER_SIZE
        if len(data) < expected_frame_size:
            raise FPrimeFrameError(
                f"Incomplete frame: got {len(data)} bytes, expected {expected_frame_size} "
                f"(payload length: {payload_length})"
            )
        
        # Extract payload and CRC
        payload = data[8:8+payload_length]
        crc = struct.unpack('>I', data[8+payload_length:8+payload_length+4])[0]
        
        frame = cls(payload, crc)
        
        # Verify CRC
        if not frame.verify_crc():
            raise FPrimeFrameError(f"CRC mismatch: calculated 0x{frame._calculate_crc():08X}, got 0x{crc:08X}")
        
        return frame


class FPrimeFrameParser:
    """Stateful parser for F Prime frames from a stream.

    This parser handles fragmented data streams and can extract multiple
    complete frames from a continuous byte stream. It maintains internal
    state to handle partial frames across buffer boundaries.
    """
    
    def __init__(self):
        """Initialize a new frame parser with empty buffer."""
        self.buffer = bytearray()
        self.state = 'searching'  # 'searching' or 'reading'
        self.expected_payload_length = 0
    
    def feed(self, data: bytes) -> list[FPrimeFrame]:
        """
        Feed data into the parser and extract complete frames.
        
        Args:
            data: New data bytes to parse
            
        Returns:
            List of complete FPrimeFrame objects
        """
        self.buffer.extend(data)
        frames = []
        
        while True:
            if self.state == 'searching':
                # Look for start word
                start_idx = self.buffer.find(FPRIME_START_WORD_BYTES)
                if start_idx == -1:
                    # No start word found, keep only last 3 bytes (might be partial start word)
                    # This handles cases where start word might span buffer boundaries
                    if len(self.buffer) > 3:
                        self.buffer = self.buffer[-3:]
                    break
                
                # If start word found but not at beginning, remove everything before it
                # This handles cases where we received garbage data before a valid frame
                if start_idx > 0:
                    logger.debug(f"Found start word at offset {start_idx}, discarding {start_idx} bytes")
                    self.buffer = self.buffer[start_idx:]
                
                # Check if we have enough data for header
                if len(self.buffer) < FRAME_HEADER_SIZE:
                    break
                
                # Read payload length
                self.expected_payload_length = struct.unpack('>I', self.buffer[4:8])[0]
                self.state = 'reading'
            
            if self.state == 'reading':
                # Check if we have complete frame
                expected_frame_size = FRAME_HEADER_SIZE + self.expected_payload_length + FRAME_FOOTER_SIZE
                if len(self.buffer) < expected_frame_size:
                    break
                
                # Try to parse frame
                try:
                    frame_data = bytes(self.buffer[:expected_frame_size])
                    frame = FPrimeFrame.from_bytes(frame_data)
                    frames.append(frame)
                    
                    # Remove parsed frame from buffer
                    self.buffer = self.buffer[expected_frame_size:]
                    self.state = 'searching'
                    self.expected_payload_length = 0
                except FPrimeFrameError as e:
                    logger.warning(f"Frame parsing error: {e}, skipping start word")
                    # Skip the start word and continue searching
                    if len(self.buffer) > 4:
                        self.buffer = self.buffer[4:]
                    self.state = 'searching'
                    self.expected_payload_length = 0
        
        return frames
    
    def reset(self) -> None:
        """Reset parser state.

        Clears internal buffer and resets to initial searching state.
        """
        self.buffer = bytearray()
        self.state = 'searching'
        self.expected_payload_length = 0
