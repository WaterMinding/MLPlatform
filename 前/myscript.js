const menu = document.getElementById("name-left");
const choice1 = document.getElementById("choice1");
const choice2 = document.getElementById("choice2");
const choice3 = document.getElementById("choice3");
const choice4 = document.getElementById("choice4");
const choice5 = document.getElementById("choice5");

menu.addEventListener("change", function() {
    const value = menu.value;
    choice1.style.display = "none";
    choice2.style.display = "none";
    choice3.style.display = "none";
    choice4.style.display = "none";
    choice5.style.display = "none";
    if (value == 1) {
        choice1.style.display = "block";

    } else if (value == 2) {
        choice2.style.display = "block";
    } else if (value == 3) {
        choice3.style.display = "block";
    } else if (value == 4) {
        choice4.style.display = "block";
         
    } else if (value == 5) {
        choice5.style.display = "block";
    }
});
