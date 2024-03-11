# Define a class
class MyClass:
    def __init__(self):
        self.counter = 0

    # Define the decorator function inside the class
    def counter_decorator(func):
        def wrapper(self, *args, **kwargs):
            # Access and increment the counter variable from the instance
            self.counter += 1
            print(f"Counter incremented to {self.counter}")

            # Call the original function
            result = func(self, *args, **kwargs)

            # Decrement the counter variable
            self.counter -= 1
            print(f"Counter decremented to {self.counter}")

            return result

        return wrapper

    # Apply the decorator to a class method
    @counter_decorator
    def my_method(self):
        print("Inside my_method")

# Create an instance of the class
obj = MyClass()

# Call the method multiple times
obj.my_method()
obj.my_method()