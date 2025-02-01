"""project :(HYDERABAD METRO) 
WE NEED TO WRITE A PYTHON SCRIPT WHICH WOULD USER INPUT AND IT SHOULD PRINT WHEATHER 
HE US AN EXSTING PASSENFER OR NEW ONE BY TAKING THE INFO FROM CUSTOMER ITSELF . 
THE SCRIPT SHOULD BE SAVED BY .MAIN"""
"""USE: INPUT () FUNCTION ALLOWING USER INPUT """

import sys
sys.path.append(r'C:\ENV\PREP\2025\python\hyd_metro_project')

from metro_card import metro_card
from passenger_details import personal_details


def display_menu():
    print("\nHi, good morning!")
    print("Could you please select one of the following options?")
    print("1: Enter 1 if you are a new passenger.")
    print("2: Enter 2 if you are an existing passenger.")
    print("3: Enter 3 to exit the program.")

def process_choice(choice):
    if choice == 1:
        print("You are a new passenger. We need to get your details.")
        metro_card()  # Call the function from metro_card module
        personal_details()  # Call the function from passenger_details module
    elif choice == 2:
        print("Thank you for your input. How may I help you?")
        # Placeholder for existing passenger functionality
        print("This feature is under development.")
    elif choice == 3:
        print("Exiting the program. Have a great day!")
        exit()
    else:
        print("Please provide the correct choice.")

if __name__ == "__main__":
    while True:
        display_menu()
        try:
            user_choice = int(input("Enter your choice: "))
            process_choice(user_choice)
        except ValueError as e:
            print(f"Invalid input: {e}. Please enter a valid number.")
