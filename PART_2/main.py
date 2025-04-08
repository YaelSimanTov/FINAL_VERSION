import pandas as pd


def generate_family_relations(input_excel: str, output_excel: str):
    """
    Generates a relationship table from a people Excel file and saves it to another file.

    :param input_excel: Path to the input Excel file (with people data)
    :param output_excel: Path to save the generated relations Excel file
    """
    # Load the Excel table
    df = pd.read_excel(input_excel)

    # Prepare an empty list for relations
    relations = []

    # Convert to dictionary for easy lookup by ID
    people_dict = df.set_index('Ρerson_Id').to_dict(orient='index')

    # Loop through each person
    for person_id, person in people_dict.items():
        gender = person['Gender']
        father_id = person['Fathеr_Id']
        mother_id = person['Mother_Id']
        spouse_id = person['Spouѕe_Id']

        # FATHER / MOTHER
        if pd.notna(father_id):
            relations.append({'Person_Id': person_id, 'Relative_Id': father_id, 'Connection_Type': 'father'})
        if pd.notna(mother_id):
            relations.append({'Person_Id': person_id, 'Relative_Id': mother_id, 'Connection_Type': 'mother'})

        # SON / DAUGHTER (reverse parenting)
        for other_id, other in people_dict.items():
            if other['Fathеr_Id'] == person_id or other['Mother_Id'] == person_id:
                child_type = 'son' if other['Gender'] == 'M' else 'daughter'
                relations.append({'Person_Id': person_id, 'Relative_Id': other_id, 'Connection_Type': child_type})

        # SPOUSE (only if spouse exists in data)
        if pd.notna(spouse_id) and spouse_id in people_dict:
            connection = 'husband' if people_dict[spouse_id]['Gender'] == 'M' else 'wife'
            relations.append({'Person_Id': person_id, 'Relative_Id': spouse_id, 'Connection_Type': connection})

        # SIBLINGS (same father or mother, different ID)
        for other_id, other in people_dict.items():
            if other_id == person_id:
                continue
            if (
                    (pd.notna(father_id) and father_id == other['Fathеr_Id']) or
                    (pd.notna(mother_id) and mother_id == other['Mother_Id'])
            ):
                sibling_type = 'brother' if other['Gender'] == 'M' else 'sister'
                relations.append({'Person_Id': person_id, 'Relative_Id': other_id, 'Connection_Type': sibling_type})

    # Create a new DataFrame and save
    relations_df = pd.DataFrame(relations)
    relations_df.drop_duplicates(inplace=True)  # Optional: remove duplicate rows
    relations_df.to_excel(output_excel, index=False)
    print(f"Relations exported to: {output_excel}")



def complete_and_save_spouses(input_excel: str, output_excel: str):
    """
    Completes missing spouse relationships in the people table.
    If person A has person B listed as a spouse, but B does not list A,
    this function fills in the missing data and saves the updated table.

    :param input_excel: Path to input Excel file with people table
    :param output_excel: Path to save updated Excel file with completed spouses
    """
    df = pd.read_excel(input_excel)

    # Set the Person ID as index for faster access
    df.set_index('Ρerson_Id', inplace=True)

    for person_id, row in df.iterrows():
        spouse_id = row['Spouѕe_Id']

        # Skip if no spouse listed
        if pd.isna(spouse_id):
            continue

        # Check if the spouse exists in the DataFrame
        if spouse_id in df.index:
            spouse_row = df.loc[spouse_id]
            # If the spouse doesn't already list this person as their spouse, fill it
            if pd.isna(spouse_row['Spouѕe_Id']):
                df.at[spouse_id, 'Spouѕe_Id'] = person_id

    # Reset index before saving
    df.reset_index(inplace=True)

    # Save the updated DataFrame to a new Excel file
    df.to_excel(output_excel, index=False)
    print(f"Updated spouse relationships saved to: {output_excel}")

if __name__ == '__main__':
    #Qusetion 1:
    generate_family_relations('PART_2/people.xlsx', 'PART_2/relations_output.xlsx')
    #Qusetion 2:
    complete_and_save_spouses('PART_2/people.xlsx', 'PART_2/people_with_completed_spouses.xlsx')
