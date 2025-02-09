"""
basic_json_consumer.py

Reads a JSON-formatted file as it is being written and visualizes author percentages.

Example JSON message:
{"message": "I just saw a movie! It was amazing.", "author": "Eve"}
"""

#####################################
# Import Modules
#####################################

import json
import os
import sys
import time
import pathlib
from collections import defaultdict
import matplotlib.pyplot as plt
from utils.utils_logger import logger  # Ensure this logger is correctly set up

#####################################
# Set up Paths - read from the file the producer writes
#####################################

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DATA_FOLDER = PROJECT_ROOT.joinpath("data")
DATA_FILE = DATA_FOLDER.joinpath("project_live.json")

logger.info(f"Project root: {PROJECT_ROOT}")
logger.info(f"Data folder: {DATA_FOLDER}")
logger.info(f"Data file: {DATA_FILE}")

#####################################
# Set up data structures
#####################################

author_counts = defaultdict(int)  # Track the count of messages per author
total_messages = 0  # Total message count
percentage_history = defaultdict(list)  # Store percentage of each author over time
message_indices = []  # Track total messages over time

#####################################
# Set up live visualization
#####################################

fig, ax = plt.subplots()
plt.ion()  # Enable interactive mode for live updates

#####################################
# Update Chart Function
#####################################


def update_chart():
    """Update the live chart with the latest author message percentages."""
    ax.clear()

    # Store the current total message count
    message_indices.append(total_messages)

    # Ensure all author lists are the same length
    for author in author_counts.keys():
        if len(percentage_history[author]) < len(message_indices):
            percentage_history[author].append((author_counts[author] / total_messages) * 100 if total_messages > 0 else 0)

    # Plot each author's percentage over time
    for author in author_counts.keys():
        ax.plot(message_indices, percentage_history[author], marker="^", linestyle="--", color="green", label=author)

    ax.set_xlabel("Total Messages Received")
    ax.set_ylabel("Percentage of Messages per Author")
    ax.set_title("Live Author Message Distribution Over Time")
    ax.legend()

    plt.tight_layout()
    plt.draw()
    plt.pause(0.01)  # Pause for real-time update



#####################################
# Process Message Function
#####################################


def process_message(message: str) -> None:
    """
    Process a single JSON message and update the chart.

    Args:
        message (str): The JSON message as a string.
    """
    global total_messages

    try:
        # Parse the JSON string into a dictionary
        message_dict = json.loads(message)
       
        if isinstance(message_dict, dict):
            author = message_dict.get("author", "unknown")  # Default to "unknown" if missing

            # Increment author count and total messages
            author_counts[author] += 1
            total_messages += 1

            logger.info(f"Total messages: {total_messages}, Current Author Counts: {dict(author_counts)}")

            # Update the chart with new data
            update_chart()

        else:
            logger.error(f"Expected a dictionary but got: {type(message_dict)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Main Function
#####################################


def main() -> None:
    """
    Main function to monitor a file for new messages and update the live chart.
    """
    logger.info("START consumer.")

    if not DATA_FILE.exists():
        logger.error(f"Data file {DATA_FILE} does not exist. Exiting.")
        sys.exit(1)

    try:
        with open(DATA_FILE, "r") as file:
            file.seek(0, os.SEEK_END)  # Move to the end of the file

            print("Consumer is ready and waiting for new JSON messages...")

            while True:
                line = file.readline()

                if line.strip():
                    process_message(line)
                else:
                    logger.debug("No new messages. Waiting...")
                    time.sleep(0.5)  # Short delay before checking again

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        plt.ioff()
        plt.show()
        logger.info("Consumer closed.")


#####################################
# Run the Consumer
#####################################

if __name__ == "__main__":
    main()
