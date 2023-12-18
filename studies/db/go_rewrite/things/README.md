# Case study data

- All exported case studies are in `case_studies_all/`/`case_studies_all_right_25` folder (in gitignore because more 87k files (>3GB))
- Random subset of all are in `case_studies_random/` folder (picked with `select_random_case_studies.ipynb` script in this directory)
- In folder `case_studies_selected/` are case studies that are hand picked from the "bottom_right" and "top_right" category with more cycles than most in the `case_studies_random/` subset (for those categories cells with many cycles are rare, so we picked some of them to have more examples of those cells)

- random top_right and bottom_right cells with threshold at 25 (cycle dependent stability) in `case_studies_random_right_25/` folder (all are in `case_studies_all_right_25` folder)

# Visualize

Use `check_case_studies.ipynb` notebook.
