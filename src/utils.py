def retrieve_file_names(lines, percentage = None):
    """
    Match .parquet and return a list. If percentage, then return the sampled list.
    
    Args:
        lines: List containing files name.
    Percentage: float 0-1
        The percentage of the total files
    """
    import re 
    from random import sample
    final_list =[]
    for l in lines:
        filename = l.split()[-1]
        match = re.search(r'([A-Z0-9_-]+.parquet)', filename)
        if match:
            final_list.append(filename)
    
    if percentage is not None:
        if percentage >= 1.0:
            raise ValueError("Percentage should be a float value between 0 and 1.")
        total_num = len(final_list)
        perc = int(percentage*total_num)
        return sample(final_list, k=perc)
    
    else: 
        return final_list   