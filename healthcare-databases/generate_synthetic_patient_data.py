import pandas as pd
import numpy as np
from faker import Faker
import random
import joblib
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

fake = Faker()
fake.seed_instance(42)
np.random.seed(42)
random.seed(42)

# Load your existing model
print("Loading stroke prediction model...")
model = joblib.load('Logistic_Regression.pkl')
print("Model loaded successfully")

# Constants for 500K rows
N = 500000

# Extended constants for comprehensive healthcare data
genders = ["Male", "Female"]
smoking_status = ["never smoked", "smokes", "formerly smoked"]
blood_types = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]
provinces = [
    "Eastern Cape", "Free State", "Gauteng", "KwaZulu-Natal", 
    "Limpopo", "Mpumalanga", "Northern Cape", "North West", "Western Cape"
]
cities_by_province = {
    "Eastern Cape": ["Port Elizabeth", "East London", "Bhisho", "Mthatha", "Queenstown"],
    "Free State": ["Bloemfontein", "Welkom", "Kroonstad", "Sasolburg", "Phuthaditjhaba"],
    "Gauteng": ["Johannesburg", "Pretoria", "Soweto", "Randburg", "Sandton", "Centurion"],
    "KwaZulu-Natal": ["Durban", "Pietermaritzburg", "Newcastle", "Richards Bay", "Ulundi"],
    "Limpopo": ["Polokwane", "Thohoyandou", "Tzaneen", "Mokopane", "Louis Trichardt"],
    "Mpumalanga": ["Nelspruit", "Witbank", "Middelburg", "Ermelo", "Barberton"],
    "Northern Cape": ["Kimberley", "Upington", "Springbok", "Kuruman", "De Aar"],
    "North West": ["Rustenburg", "Potchefstroom", "Klerksdorp", "Mahikeng", "Lichtenburg"],
    "Western Cape": ["Cape Town", "Stellenbosch", "George", "Paarl", "Worcester"]
}

departments = ["Cardiology", "Neurology", "Emergency", "Internal Medicine", "ICU", "General Ward"]
insurance_types = ["Private", "Medical Aid", "Government", "Self-pay", "Corporate"]
work_types = ["Private", "Self-employed", "Government", "Never worked", "Children"]
residence_types = ["Urban", "Rural"]
diet_types = ["Balanced", "Vegetarian", "High-protein", "Low-carb", "Mediterranean", "Fast-food"]
exercise_freq = ["Sedentary", "Light", "Moderate", "Intense", "Athletic"]
stress_levels = ["Low", "Moderate", "High", "Severe"]
sleep_categories = ["<5h", "5-6h", "6-7h", "7-8h", "8-9h", ">9h"]

def generate_realistic_vitals(age, gender, smoking, hypertension, heart_disease):
    """Generate realistic vital signs based on health conditions"""
    # Base values
    heart_rate = 72 + np.random.normal(0, 8)
    temp = 36.5 + np.random.normal(0, 0.3)
    oxygen_sat = 98 + np.random.normal(0, 1)
    
    # Adjust for conditions
    if smoking == "smokes":
        heart_rate += random.uniform(5, 15)
        oxygen_sat -= random.uniform(1, 3)
    if hypertension:
        heart_rate += random.uniform(3, 8)
    if heart_disease:
        heart_rate += random.uniform(5, 12)
        oxygen_sat -= random.uniform(0.5, 2)
    if age > 65:
        heart_rate += random.uniform(0, 5)
        oxygen_sat -= random.uniform(0, 1)
    
    return {
        "heart_rate": max(50, min(120, int(heart_rate))),
        "temperature": max(35.0, min(39.0, round(temp, 1))),
        "oxygen_saturation": max(90, min(100, int(oxygen_sat)))
    }

def generate_glucose_and_bmi(age, hypertension, heart_disease, smoking, gender):
    """Generate realistic glucose and BMI with correlations"""
    # Glucose generation
    base_glucose = 90 + (0.5 * hypertension + 0.7 * heart_disease) * 25
    glucose_noise = np.random.normal(0, 12)
    glucose = base_glucose + glucose_noise + (0.08 * age)
    
    # BMI generation
    if gender == "Male":
        base_bmi = random.uniform(20, 32)
    else:
        base_bmi = random.uniform(18, 30)
    
    if smoking == "smokes":
        base_bmi -= random.uniform(0, 1.5)
    if age > 50:
        base_bmi += random.uniform(0, 2.5)
    
    bmi_noise = np.random.normal(0, 1.5)
    bmi = base_bmi + bmi_noise
    
    return {
        "avg_glucose_level": max(70, min(280, round(glucose, 1))),
        "bmi": max(15, min(45, round(bmi, 1)))
    }

def calculate_age(date_of_birth_str):
    """Calculate age from date of birth string"""
    from datetime import datetime
    birth_date = datetime.strptime(date_of_birth_str, "%Y-%m-%d")
    today = datetime.now()
    age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
    return age

def generate_stroke_outcome_with_model(patient_data):
    """Use your actual model to predict stroke probability"""
    try:
        # Calculate age from date_of_birth since it's not directly available
        current_age = calculate_age(patient_data['date_of_birth'])
        
        # Prepare data for model - ensure all required fields are present
        model_input_data = {
            'gender': patient_data['gender'],
            'age': float(current_age),  # Calculate age from date_of_birth
            'hypertension': int(patient_data['hypertension']),
            'heart_disease': int(patient_data['heart_disease']),
            'avg_glucose_level': float(patient_data['avg_glucose_level']),
            'bmi': float(patient_data['bmi']),
            'smoking_status': patient_data['smoking_status'],
            'name': patient_data['name'],
            'country': patient_data['country'],
            'province': patient_data['province'],
            'age_group': patient_data['age_group'],
            'bmi_category': patient_data['bmi_category'],
            'glucose_category': patient_data['glucose_category'],
            'age_hypertension': float(current_age * patient_data['hypertension'])  # Calculate this too
        }
        
        # Create DataFrame with correct column order and types
        model_input = pd.DataFrame([model_input_data])
        
        # Get probability from your model
        stroke_probability = float(model.predict_proba(model_input)[0][1])
        
    except Exception as e:
        print(f"Model prediction failed for patient: {e}")
        # Fallback to realistic probability based on risk factors
        current_age = calculate_age(patient_data['date_of_birth'])
        
        base_prob = 0.05  # Base stroke probability
        
        # Add risk factors
        if current_age > 65:
            base_prob += 0.15
        if patient_data['hypertension']:
            base_prob += 0.12
        if patient_data['heart_disease']:
            base_prob += 0.18
        if patient_data['avg_glucose_level'] > 140:
            base_prob += 0.08
        if patient_data['bmi'] > 30:
            base_prob += 0.06
        if patient_data['smoking_status'] == "smokes":
            base_prob += 0.10
        elif patient_data['smoking_status'] == "formerly smoked":
            base_prob += 0.05
            
        stroke_probability = min(0.9, max(0.01, base_prob))
    
    # Add some realistic variation (model isn't perfect)
    final_probability = stroke_probability * random.uniform(0.9, 1.1)
    final_probability = max(0.01, min(0.95, final_probability))
    
    # Calculate age for severity determination
    current_age = calculate_age(patient_data['date_of_birth'])
    
    # Determine actual stroke occurrence (binary outcome)
    stroke_occurred = np.random.random() < final_probability
    
    # Determine stroke severity if stroke occurred
    stroke_severity = None
    stroke_date = None
    if stroke_occurred:
        # Higher probability patients tend to have more severe strokes
        severity_rand = np.random.random()
        if final_probability > 0.7:
            stroke_severity = "Severe" if severity_rand < 0.5 else "Moderate"
        elif final_probability > 0.4:
            stroke_severity = "Moderate" if severity_rand < 0.6 else "Mild"
        else:
            stroke_severity = "Mild" if severity_rand < 0.7 else "Moderate"
        
        # Generate stroke date within last 2 years
        days_ago = random.randint(1, 730)
        stroke_date = datetime.now() - timedelta(days=days_ago)
    
    recovery_status = None
    if stroke_occurred:
        if stroke_severity == "Mild":
            recovery_status = random.choice(["Full", "Partial"])
        elif stroke_severity == "Moderate":
            recovery_status = random.choice(["Partial", "Ongoing"])
        else:  # Severe
            recovery_status = random.choice(["Ongoing", "Permanent disability"])
    
    return {
        "stroke_probability": final_probability,
        "stroke_occurred": int(stroke_occurred),
        "stroke_severity": stroke_severity,
        "stroke_date": stroke_date.strftime("%Y-%m-%d") if stroke_date else None,
        "recovery_status": recovery_status
    }

print("Starting synthetic data generation for 500,000 patients...")
print("This will take several minutes...")

# Generate data in chunks to manage memory
chunk_size = 10000
total_chunks = N // chunk_size
all_data = []

for chunk in range(total_chunks):
    print(f"Generating chunk {chunk + 1}/{total_chunks}...")
    chunk_data = []
    
    for i in range(chunk_size):
        # Basic demographics
        gender = random.choice(genders)
        age = random.randint(18, 95)
        
        # Generate name based on gender and ethnicity (South African context)
        if random.random() < 0.7:  # 70% South African names
            if gender == "Male":
                first_names = ["Johannes", "Pieter", "Jacobus", "Willem", "David", "Lukas", "Andries", "Hendrik", "Gabriel", "Moses"]
                last_names = ["van der Merwe", "Botha", "du Plessis", "van Niekerk", "Joubert", "Cronje", "Steyn", "Viljoen", "Kruger", "Coetzee"]
            else:
                first_names = ["Anna", "Elizabeth", "Maria", "Johanna", "Magdalena", "Susanna", "Catharina", "Helena", "Sophia", "Petronella"]
                last_names = ["van der Merwe", "Botha", "du Plessis", "van Niekerk", "Joubert", "Cronje", "Steyn", "Viljoen", "Kruger", "Coetzee"]
        else:  # 30% other ethnicities
            if gender == "Male":
                first_name = fake.first_name_male()
            else:
                first_name = fake.first_name_female()
            last_name = fake.last_name()
            first_names = [first_name]
            last_names = [last_name]
        
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        full_name = f"{first_name} {last_name}"
        
        # Geographic data
        province = random.choice(provinces)
        city = random.choice(cities_by_province[province])
        
        # Health conditions with realistic correlations
        hypertension = random.choice([0, 1])
        heart_disease = random.choice([0, 1])
        
        # Increase probability with age
        if age > 60:
            hypertension = random.choice([0, 0, 1])  # 67% chance
            heart_disease = random.choice([0, 0, 1])  # 67% chance
        
        smoking = random.choice(smoking_status)
        
        # Generate vitals and measurements
        vitals = generate_realistic_vitals(age, gender, smoking, hypertension, heart_disease)
        measurements = generate_glucose_and_bmi(age, hypertension, heart_disease, smoking, gender)
        
        # Generate additional health data
        diabetes = random.choice([0, 1])
        if age > 50 and (hypertension or heart_disease):
            diabetes = random.choice([0, 1, 1])  # Higher chance
        
        cholesterol_base = 180 + (30 if age > 40 else 0) + (20 if diabetes else 0) + (15 if heart_disease else 0)
        cholesterol_level = max(120, min(350, int(cholesterol_base + np.random.normal(0, 25))))
        
        # Blood pressure
        if hypertension:
            systolic = random.randint(140, 180)
            diastolic = random.randint(90, 110)
        else:
            systolic = random.randint(110, 130)
            diastolic = random.randint(70, 85)
        
        # Lifestyle data
        alcohol = random.choice(["None", "Occasional", "Moderate", "Heavy"])
        exercise = random.choice(exercise_freq)
        diet = random.choice(diet_types)
        stress = random.choice(stress_levels)
        sleep = random.choice(sleep_categories)
        
        # Social/work data
        work_type = random.choice(work_types)
        if age < 18:
            work_type = "Children"
        elif age > 65:
            work_type = random.choice(["Never worked", "Private", "Self-employed"])
        
        ever_married = random.choice(["Yes", "No"])
        if age < 25:
            ever_married = random.choice(["Yes", "No", "No"])
        residence_type = random.choice(residence_types)
        
        # Healthcare system data
        hospital_id = f"H{random.randint(1000, 9999)}"
        department = random.choice(departments)
        insurance = random.choice(insurance_types)
        
        # Generate dates
        registration_date = datetime.now() - timedelta(days=random.randint(30, 1825))
        visit_date = registration_date + timedelta(days=random.randint(1, 365))
        admission_date = visit_date if random.random() < 0.3 else None
        
        # Create patient record
        patient_data = {
            # Core identifiers
            'patient_id': f"P{chunk * chunk_size + i + 1:08d}",
            'first_name': first_name,
            'last_name': last_name,
            'date_of_birth': (datetime.now() - timedelta(days=age*365 + random.randint(0, 365))).strftime("%Y-%m-%d"),
            'gender': gender,
            'blood_type': random.choice(blood_types),
            
            # Geographic
            'province': province,
            'city': city,
            'postal_code': f"{random.randint(1000, 9999)}",
            'healthcare_region': f"Region_{random.randint(1, 5)}",
            
            # Medical conditions
            'hypertension': hypertension,
            'heart_disease': heart_disease,
            'diabetes': diabetes,
            'cholesterol_level': cholesterol_level,
            'blood_pressure_systolic': systolic,
            'blood_pressure_diastolic': diastolic,
            
            # Clinical measurements
            'avg_glucose_level': measurements['avg_glucose_level'],
            'bmi': measurements['bmi'],
            'heart_rate': vitals['heart_rate'],
            'temperature': vitals['temperature'],
            'oxygen_saturation': vitals['oxygen_saturation'],
            'height_cm': random.randint(150, 190) if gender == "Male" else random.randint(145, 175),
            'weight_kg': int(measurements['bmi'] * (random.randint(150, 190)/100)**2),
            
            # Lifestyle
            'smoking_status': smoking,
            'alcohol_consumption': alcohol,
            'exercise_frequency': exercise,
            'diet_type': diet,
            'stress_level': stress,
            'sleep_hours': sleep,
            
            # Social/Work
            'ever_married': ever_married,
            'work_type': work_type,
            'residence_type': residence_type,
            
            # Healthcare system
            'hospital_id': hospital_id,
            'medical_record_number': f"MRN{random.randint(100000, 999999)}",
            'department': department,
            'attending_physician': f"Dr. {fake.last_name()}",
            'primary_care_provider': f"Dr. {fake.last_name()}",
            'insurance_type': insurance,
            'insurance_id': f"INS{random.randint(100000, 999999)}",
            
            # Dates
            'registration_date': registration_date.strftime("%Y-%m-%d"),
            'visit_date': visit_date.strftime("%Y-%m-%d"),
            'admission_date': admission_date.strftime("%Y-%m-%d") if admission_date else None,
            'discharge_status': random.choice(["Discharged", "Admitted", "Transferred"]) if admission_date else None,
            
            # Derived features for model
            'age_group': "Young adult" if age < 50 else "Middle-aged" if age < 80 else "Very old",
            'bmi_category': "Underweight" if measurements['bmi'] < 18.5 else "Healthy Weight" if measurements['bmi'] < 25 else "Overweight" if measurements['bmi'] < 30 else "Obese",
            'glucose_category': "Low" if measurements['avg_glucose_level'] < 100 else "Normal" if measurements['avg_glucose_level'] < 140 else "High",
            'age_hypertension': age * hypertension,
            'country': "South Africa",
            'name': full_name
        }
        
        # Generate stroke outcome using your model
        stroke_result = generate_stroke_outcome_with_model(patient_data)
        patient_data.update(stroke_result)
        
        chunk_data.append(patient_data)
    
    all_data.extend(chunk_data)
    print(f"Chunk {chunk + 1} completed. Total progress: {len(all_data)}/{N}")

# Convert to DataFrame
print("Creating DataFrame...")
df = pd.DataFrame(all_data)

# Add some final calculated fields
print("Adding calculated fields...")
df['risk_factors_combined'] = (df['hypertension'] + df['heart_disease'] + df['diabetes']).astype(str) + '_factors'
df['bed_number'] = df.apply(lambda x: f"B{random.randint(100, 999)}" if x['admission_date'] else None, axis=1)
df['room_number'] = df.apply(lambda x: f"R{random.randint(100, 999)}" if x['admission_date'] else None, axis=1)

# Save to CSV (this will be a large file)
print("Saving to CSV...")
df.to_csv('synthetic_healthcare_500k_stroke_model.csv', index=False)

print(f"✅ Generated {len(df)} synthetic healthcare records")
print(f"📊 Stroke occurrence rate: {df['stroke_occurred'].mean():.2%}")
print(f"📊 Average stroke probability: {df['stroke_probability'].mean():.3f}")
print(f"📁 File saved as: synthetic_healthcare_500k_stroke_model.csv")

# Display column summary
print("\n📋 Column Summary:")
for col in df.columns:
    non_null = df[col].notna().sum()
    print(f"  {col}: {non_null:,} non-null values")

# Display stroke severity distribution
if df['stroke_occurred'].sum() > 0:
    print("\n🎯 Stroke Severity Distribution:")
    severity_counts = df[df['stroke_occurred'] == 1]['stroke_severity'].value_counts()
    for severity, count in severity_counts.items():
        print(f"  {severity}: {count:,} ({count/len(df)*100:.2f}%)")